#include <inttypes.h>

#include "esp_heap_caps.h"
#include "esp_log.h"
#include "esp_system.h"

#include "heapwatch.h"

static const char *TAG = "heapwatch";

static uint32_t s_low;          /* day thap nhat da thay */
static const char *s_low_where; /* cho da lam no thap nhu vay */

void heap_mark(const char *where)
{
    /* esp_get_minimum_free_heap_size() la day KE TU LUC BOOT cua toan he,
     * nen khi no tut nghia la vua co ai do cap phat lon. So sanh voi lan
     * truoc de chi in khi CO DAY MOI. */
    uint32_t now = esp_get_minimum_free_heap_size();
    if (s_low != 0 && now >= s_low) {
        return;
    }
    uint32_t drop = (s_low == 0) ? 0 : (s_low - now);
    s_low = now;
    s_low_where = where;
    ESP_LOGW(TAG, "day moi %" PRIu32 " B tai '%s' (tut %" PRIu32 " B, trong %" PRIu32 ")",
             now, where, drop, (uint32_t)esp_get_free_heap_size());
}
