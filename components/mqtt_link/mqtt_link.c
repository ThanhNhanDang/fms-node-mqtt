#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "freertos/FreeRTOS.h"
#include "freertos/queue.h"
#include "freertos/task.h"

#include "esp_log.h"
#include "esp_task_wdt.h"
#include "esp_timer.h"
#include "mqtt_client.h"

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
uint32_t  mqtt_link_suppressed(void){ return 0; }
bool      mqtt_link_connected(void) { return false; }

#else

/* 256 ban ghi (5,6 KB) la thua: sau khi co bao-khi-doi nhip chi con ~1/s.
 * 32 van du hap thu mot con bung khi ca day chuyen doi trang thai cung luc. */
#define TAP_QUEUE_LEN 32
#define TOPIC_MAX     160

/* So kenh toi da theo doi cho RBE. cfg cho phep nhieu hon; kenh vuot bang
 * nay khong bi loc (luon phat) — an toan theo huong "phat thua con hon bo
 * sot". */
#define RBE_MAX_CHANNEL 32

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
/* ── bao-khi-doi (report by exception) ──────────────────────────────────
 *
 * Do ngay tren duong day nay 18/09: count1 · pedal1 · count2 phat so 0 moi
 * 200 ms va chiem 75% luu luong. Phat lai mot gia tri khong doi khong noi
 * them dieu gi, nhung no chiem song, va chinh loai lang phi do da lam sap
 * instance Odoo sang cung ngay.
 *
 * Quy tac: phat khi (a) gia tri doi qua vung chet, (b) chat luong doi,
 * hoac (c) da im qua lau.
 *
 * (c) KHONG duoc bo. Kenh ben Odoo co `max_age_ms`; im lang qua nguong do
 * thi o gia tri chuyen xam va nut [Dat] bi chan — da gap that voi
 * scale_esp32 (max_age_ms 1500 ms trong khi node day moi 5 s). Nen gia tri
 * dung yen van phai duoc nhac lai dinh ky.
 */
static float    s_rbe_val[RBE_MAX_CHANNEL];
static uint8_t  s_rbe_q[RBE_MAX_CHANNEL];
static int64_t  s_rbe_us[RBE_MAX_CHANNEL];
static bool     s_rbe_seen[RBE_MAX_CHANNEL];
static uint32_t s_suppressed;

static bool rbe_should_send(const measurement_t *m)
{
    if (m->channel_id >= RBE_MAX_CHANNEL) {
        return true;                    /* ngoai bang: khong loc */
    }
    const uint16_t i = m->channel_id;
    const int64_t now = esp_timer_get_time();

    if (!s_rbe_seen[i]) {
        goto send;                      /* mau dau tien cua kenh */
    }
    if (m->quality != s_rbe_q[i]) {
        goto send;
    }
    if ((now - s_rbe_us[i]) >= (int64_t)CONFIG_MQTT_LINK_MAX_SILENCE_MS * 1000) {
        goto send;                      /* nhac lai dinh ky */
    }
    {
        const float d = m->value - s_rbe_val[i];
        const float band = (float)CONFIG_MQTT_LINK_DEADBAND_MILLI / 1000.0f;
        if ((d < 0 ? -d : d) > band) {
            goto send;
        }
    }
    return false;

send:
    s_rbe_val[i]  = m->value;
    s_rbe_q[i]    = m->quality;
    s_rbe_us[i]   = now;
    s_rbe_seen[i] = true;
    return true;
}

static void tap_cb(const measurement_t *m)
{
    if (!rbe_should_send(m)) {
        __atomic_add_fetch(&s_suppressed, 1, __ATOMIC_RELAXED);
        return;
    }
    if (s_q != NULL && xQueueSend(s_q, m, 0) != pdTRUE) {
        __atomic_add_fetch(&s_dropped, 1, __ATOMIC_RELAXED);
    }
}

/* ── gom mot lo thanh JSON, KHONG dung cJSON ────────────────────────────
 *
 * Ban dau toi bat chuoc build_measurements_body() cua uplink.c: dung cay
 * cJSON roi in ra chuoi. Tren con node nay do la sai lam.
 *
 * Mot cay cJSON cho 50 ban ghi la ~350 nut x ~64 byte = ~22 KB NHAT THOI,
 * cong chuoi in ra, cong bo dem cua cJSON_Print tu nhan doi khi day. Ma
 * uplink cung dung dung cach do cung luc — hai cay tren mot heap 48 KB.
 * Ket qua do duoc 19/09: min_heap tut con 448 byte, upload_batch khong cap
 * phat noi bo dem HTTP, `sent=0` va spool day cung 2048.
 *
 * Goi nay co hinh dang co dinh, khong can cay doi tuong. Viet thang vao
 * mot bo dem TINH: khong cap phat, khong phan manh, tran thi cat lo.
 *
 * Hinh dang giu nguyen 100% so voi POST /node/v1/measurements — do van la
 * ly do consumer khong can bo phan tich thu hai.
 */
static char s_body[CONFIG_MQTT_LINK_BODY_BYTES];

/* Tra ve so ban ghi da viet duoc (co the < n neu bo dem day), 0 neu khong
 * viet duoc gi. Do dai chuoi nam o *out_len. */
static size_t build_body(const measurement_t *batch, size_t n, size_t *out_len)
{
    const int64_t offset = net_mgr_time_offset_ms();
    int w = snprintf(s_body, sizeof(s_body), "{\"bid\":%u,\"seq\":%" PRIu32 ",\"items\":[",
                     (unsigned)batch[n - 1].boot_id, batch[n - 1].seq);
    if (w < 0 || (size_t)w >= sizeof(s_body)) {
        return 0;
    }
    size_t len = (size_t)w;
    size_t done = 0;

    for (size_t i = 0; i < n; i++) {
        const measurement_t *m  = &batch[i];
        const cfg_channel_t *ch = cfg_channel_by_id(m->channel_id);
        if (ch == NULL) {
            continue;
        }
        int64_t ts = m->ts_ms;
        if (ts < EPOCH_SANE_MS) {
            ts += offset;   /* ghi truoc lan dong bo SNTP dau tien */
        }
        int k = snprintf(s_body + len, sizeof(s_body) - len,
                         "%s{\"ch\":\"%s\",\"v\":%.4f,\"s\":null,\"q\":%u,"
                         "\"ts\":%lld,\"stable\":%s}",
                         done ? "," : "", ch->code, (double)m->value,
                         (unsigned)m->quality, (long long)ts,
                         m->quality != Q_UNSTABLE ? "true" : "false");
        if (k < 0 || (size_t)k >= sizeof(s_body) - len) {
            break;          /* day bo dem: gui nhung gi da co, phan con lai
                             * o lai hang doi cho lo sau */
        }
        len += (size_t)k;
        done++;
    }
    if (done == 0) {
        return 0;
    }
    int k = snprintf(s_body + len, sizeof(s_body) - len, "]}");
    if (k < 0 || (size_t)k >= sizeof(s_body) - len) {
        return 0;
    }
    *out_len = len + (size_t)k;
    return done;
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
        /* Mac dinh cua esp-mqtt la 1024/1024 va ngan xep 6144 — rong rai
         * cho mot thiet bi thong thuong, qua tay cho con node nay (heap
         * trong chi 48 KB, xem ghi chu dau ham build_body). Goi ra lon
         * nhat la s_body; goi vao chi la PUBACK va lenh, vai chuc byte. */
        .buffer.size                   = 512,
        .buffer.out_size               = CONFIG_MQTT_LINK_BODY_BYTES + 256,
        .task.stack_size               = 3584,
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

        size_t body_len = 0;
        size_t done = build_body(batch, n, &body_len);
        if (done == 0) {
            __atomic_add_fetch(&s_dropped, n, __ATOMIC_RELAXED);
            continue;
        }
        /* enqueue, KHONG publish.
         *
         * esp_mqtt_client_publish() o QoS 1 CHAN tac vu goi cho toi khi co
         * PUBACK. enqueue() bo goi vao hang cua tac vu mang roi tra ve
         * ngay; QoS 1 va PUBACK van nguyen. esp-mqtt CHEP payload vao
         * outbox cua no, nen dung bo dem tinh o day la an toan. */
        int msg_id = esp_mqtt_client_enqueue(s_cli, s_topic_meas, s_body,
                                             (int)body_len, 1, 0, true);
        if (msg_id < 0) {
            __atomic_add_fetch(&s_dropped, done, __ATOMIC_RELAXED);
            ESP_LOGW(TAG, "enqueue that bai, bo %u ban ghi", (unsigned)done);
        } else {
            __atomic_add_fetch(&s_published, done, __ATOMIC_RELAXED);
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

    /* Uu tien 4 — THAP HON uplink (5) va cung loi 0. Duong HTTP van la
     * duong chinh thuc cho toi khi cat han; khi hai ben tranh song thi
     * uplink phai thang. */
    /* 3072 du: dung chuoi bang snprintf vao bo dem TINH, khong co cay
     * cJSON nao tren ngan xep nua. Uu tien 4 — THAP HON uplink (5). */
    BaseType_t ok = xTaskCreatePinnedToCore(link_task, "mqtt_link", 3072,
                                            NULL, 4, NULL, 0);
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

uint32_t mqtt_link_suppressed(void)
{
    return __atomic_load_n(&s_suppressed, __ATOMIC_RELAXED);
}

bool mqtt_link_connected(void)
{
    return s_connected;
}

#endif /* CONFIG_MQTT_LINK_ENABLE */
