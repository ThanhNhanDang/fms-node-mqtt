#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"

#include "esp_log.h"
#include "esp_task_wdt.h"
#include "mqtt_client.h"

#include "cJSON.h"

#include "cfg.h"
#include "meas_core.h"
#include "mqtt_link.h"
#include "net_mgr.h"

/* Tat trong menuconfig thi ca file thanh mot bo stub.
 *
 * Phai bao het ca file chu khong chi rieng mqtt_link_start(): cac lua chon
 * Kconfig khac deu `depends on MQTT_LINK_ENABLE`, nen khi tat chung KHONG
 * ton tai — moi cho tham chieu CONFIG_MQTT_LINK_BROKER_URI hay
 * CONFIG_MQTT_LINK_BATCH_MAX se lam vo build. */
#if !CONFIG_MQTT_LINK_ENABLE

esp_err_t mqtt_link_start(void)     { return ESP_OK; }
uint32_t  mqtt_link_published(void) { return 0; }
uint32_t  mqtt_link_dropped(void)   { return 0; }
bool      mqtt_link_connected(void) { return false; }

#else

#define TAP_QUEUE_LEN 256
#define TOPIC_MAX     160

/* Truoc moc nay coi nhu dong ho chua dong bo SNTP — giong het nguong
 * uplink.c dung, de hai duong gan nhan thoi gian nhu nhau. */
#define EPOCH_SANE_MS 1600000000000LL

static const char *TAG = "mqttlink";

static QueueHandle_t            s_q;
static esp_mqtt_client_handle_t s_cli;
static volatile bool            s_connected;
static uint32_t                 s_published;
static uint32_t                 s_dropped;
static char                     s_topic_meas[TOPIC_MAX];
static char                     s_topic_status[TOPIC_MAX];

/* ── cai tap gan vao meas_core ──────────────────────────────────────────
 *
 * Chay TREN TASK DO (mb_tcp, scale_serial, io_scan...), nen tuyet doi
 * khong duoc chan. Hang doi day thi bo va dem — o giai doan nay HTTP van
 * gui du du lieu, mat o day khong mat that.
 */
static void tap_cb(const measurement_t *m)
{
    if (s_q != NULL && xQueueSend(s_q, m, 0) != pdTRUE) {
        __atomic_add_fetch(&s_dropped, 1, __ATOMIC_RELAXED);
    }
}

/* ── gom mot lo thanh JSON ──────────────────────────────────────────────
 *
 * Dung DUNG hinh dang than goi cua POST /node/v1/measurements (xem
 * build_measurements_body trong uplink.c): {bid, seq, items:[{ch,v,s,q,
 * ts,stable}]}. Giu nguyen co chu dich: consumer ben edge tai dung lai
 * bo phan tich san co, khong phai viet hai duong doc khac nhau.
 */
static char *build_body(const measurement_t *batch, size_t n)
{
    cJSON *root = cJSON_CreateObject();
    if (root == NULL) {
        return NULL;
    }
    cJSON_AddNumberToObject(root, "bid", batch[n - 1].boot_id);
    cJSON_AddNumberToObject(root, "seq", batch[n - 1].seq);
    cJSON *items = cJSON_AddArrayToObject(root, "items");

    const int64_t offset = net_mgr_time_offset_ms();
    for (size_t i = 0; i < n; i++) {
        const measurement_t  *m  = &batch[i];
        const cfg_channel_t  *ch = cfg_channel_by_id(m->channel_id);
        if (ch == NULL) {
            continue;
        }
        int64_t ts = m->ts_ms;
        if (ts < EPOCH_SANE_MS) {
            ts += offset; /* ghi truoc lan dong bo SNTP dau tien */
        }
        cJSON *it = cJSON_CreateObject();
        cJSON_AddStringToObject(it, "ch", ch->code);
        cJSON_AddNumberToObject(it, "v", m->value);
        cJSON_AddNullToObject(it, "s");
        cJSON_AddNumberToObject(it, "q", m->quality);
        cJSON_AddNumberToObject(it, "ts", (double)ts);
        cJSON_AddBoolToObject(it, "stable", m->quality != Q_UNSTABLE);
        cJSON_AddItemToArray(items, it);
    }

    char *body = cJSON_PrintUnformatted(root);
    cJSON_Delete(root);
    return body;
}

static void on_mqtt_event(void *handler_args, esp_event_base_t base,
                          int32_t event_id, void *event_data)
{
    (void)handler_args;
    (void)base;
    (void)event_data;

    switch ((esp_mqtt_event_id_t)event_id) {
    case MQTT_EVENT_CONNECTED:
        s_connected = true;
        /* Bao con song, retained: ai dang ky sau van doc duoc trang thai
         * hien tai ma khong phai cho goi ke tiep. Cap voi Last Will o
         * duoi — broker tu phat "online:false" khi node rot, khong ton
         * mot goi heartbeat nao. Day la thu HTTP khong lam duoc. */
        esp_mqtt_client_publish(s_cli, s_topic_status,
                                "{\"online\":true}", 0, 1, 1);
        ESP_LOGI(TAG, "da noi broker");
        break;

    case MQTT_EVENT_DISCONNECTED:
        s_connected = false;
        ESP_LOGW(TAG, "mat ket noi broker");
        break;

    case MQTT_EVENT_ERROR:
        ESP_LOGW(TAG, "loi mqtt");
        break;

    default:
        break;
    }
}

static void link_task(void *arg)
{
    (void)arg;
    ESP_ERROR_CHECK(esp_task_wdt_add(NULL));

    /* Cho co IP roi hay mo ket noi. esp-mqtt tu thu lai duoc, nhung cho
     * o day thi nhat ky sach hon va khong ban mot vong thu vo ich luc
     * moi boot. Cho theo lat nho de con nuoi watchdog. */
    while ((net_mgr_bits() & NET_BIT_WIFI) == 0) {
        vTaskDelay(pdMS_TO_TICKS(500));
        esp_task_wdt_reset();
    }

    esp_mqtt_client_config_t cc = {
        .broker.address.uri            = CONFIG_MQTT_LINK_BROKER_URI,
        .credentials.client_id         = cfg_node_serial(),
        .credentials.username          = CONFIG_MQTT_LINK_USERNAME,
        .credentials.authentication.password = CONFIG_MQTT_LINK_PASSWORD,
        .session.keepalive             = 30,
        .session.last_will.topic       = s_topic_status,
        .session.last_will.msg         = "{\"online\":false}",
        .session.last_will.qos         = 1,
        .session.last_will.retain      = 1,
        .network.reconnect_timeout_ms  = 5000,
    };

    s_cli = esp_mqtt_client_init(&cc);
    if (s_cli == NULL) {
        ESP_LOGE(TAG, "khong tao duoc client, dung component");
        esp_task_wdt_delete(NULL);
        vTaskDelete(NULL);
        return;
    }
    esp_mqtt_client_register_event(s_cli, ESP_EVENT_ANY_ID, on_mqtt_event, NULL);
    esp_mqtt_client_start(s_cli);

    measurement_t *batch = calloc(CONFIG_MQTT_LINK_BATCH_MAX,
                                  sizeof(measurement_t));
    if (batch == NULL) {
        ESP_LOGE(TAG, "khong du RAM cho lo %d ban ghi",
                 CONFIG_MQTT_LINK_BATCH_MAX);
        esp_task_wdt_delete(NULL);
        vTaskDelete(NULL);
        return;
    }

    while (1) {
        /* Gom toi khi day lo HOAC het thoi gian cho — cai nao truoc.
         * FLUSH_MS nho thi so ve nhanh, nhung goi vun; day la cho chinh
         * de danh doi giua do tuoi va so luong goi. */
        size_t     n        = 0;
        TickType_t deadline = xTaskGetTickCount() +
                              pdMS_TO_TICKS(CONFIG_MQTT_LINK_FLUSH_MS);
        while (n < (size_t)CONFIG_MQTT_LINK_BATCH_MAX) {
            TickType_t now = xTaskGetTickCount();
            if (now >= deadline) {
                break;
            }
            TickType_t wait = deadline - now;
            if (wait > pdMS_TO_TICKS(200)) {
                wait = pdMS_TO_TICKS(200); /* lat nho de con reset wdt */
            }
            if (xQueueReceive(s_q, &batch[n], wait) == pdTRUE) {
                n++;
            }
            esp_task_wdt_reset();
        }
        esp_task_wdt_reset();

        if (n == 0) {
            continue;
        }
        if (!s_connected) {
            __atomic_add_fetch(&s_dropped, n, __ATOMIC_RELAXED);
            continue;
        }

        char *body = build_body(batch, n);
        if (body == NULL) {
            __atomic_add_fetch(&s_dropped, n, __ATOMIC_RELAXED);
            continue;
        }
        /* QoS 1: broker phai PUBACK. Day chinh la cai "ack" ma spooler
         * von trong cho tu HTTP — nen khi cat HTTP o giai doan sau,
         * spool_ack_through() gan vao day duoc ma khong doi kien truc. */
        int msg_id = esp_mqtt_client_publish(s_cli, s_topic_meas, body,
                                             0, 1, 0);
        free(body);

        if (msg_id < 0) {
            __atomic_add_fetch(&s_dropped, n, __ATOMIC_RELAXED);
            ESP_LOGW(TAG, "publish that bai, bo %u ban ghi", (unsigned)n);
        } else {
            __atomic_add_fetch(&s_published, n, __ATOMIC_RELAXED);
        }
    }
}

esp_err_t mqtt_link_start(void)
{
    if (CONFIG_MQTT_LINK_BROKER_URI[0] == '\0') {
        ESP_LOGW(TAG, "chua dat broker URI, bo qua (node van chay HTTP)");
        return ESP_OK;
    }

    const char *serial = cfg_node_serial();
    snprintf(s_topic_meas, sizeof(s_topic_meas), "%s/%s/meas",
             CONFIG_MQTT_LINK_TOPIC_BASE, serial);
    snprintf(s_topic_status, sizeof(s_topic_status), "%s/%s/status",
             CONFIG_MQTT_LINK_TOPIC_BASE, serial);

    s_q = xQueueCreate(TAP_QUEUE_LEN, sizeof(measurement_t));
    if (s_q == NULL) {
        return ESP_ERR_NO_MEM;
    }

    esp_err_t err = meas_add_tap(tap_cb);
    if (err != ESP_OK) {
        vQueueDelete(s_q);
        s_q = NULL;
        return err;
    }

    BaseType_t ok = xTaskCreatePinnedToCore(link_task, "mqtt_link", 5120,
                                            NULL, 5, NULL, 0);
    if (ok != pdPASS) {
        return ESP_ERR_NO_MEM;
    }
    ESP_LOGI(TAG, "phat len %s", s_topic_meas);
    return ESP_OK;
}

uint32_t mqtt_link_published(void)
{
    return __atomic_load_n(&s_published, __ATOMIC_RELAXED);
}

uint32_t mqtt_link_dropped(void)
{
    return __atomic_load_n(&s_dropped, __ATOMIC_RELAXED);
}

bool mqtt_link_connected(void)
{
    return s_connected;
}

#endif /* CONFIG_MQTT_LINK_ENABLE */
