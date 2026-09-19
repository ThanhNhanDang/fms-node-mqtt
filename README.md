# fms-node-mqtt

Đưa node FMS (ESP32-S3) và `pcm-edge-collector` **sang hẳn MQTT**. HTTP vẫn
còn trong mã nguồn, nhưng là một tuỳ chọn biên dịch và hiện đang **tắt**.

Dành cho firmware `fms-node` (ESP-IDF, target `esp32s3`) nói chuyện với
`pcm-edge-collector`.

---

## Đo được trên hệ thật

Node `68EE8F4F06A8`, dây chuyền RL-100, 19/09/2026.

| | HTTP (trước) | MQTT (nay) |
|--|--|--|
| Độ trễ một lệnh bật đèn | 2,2 – 2,6 s | **0,133 s** (trung vị 10 lần, max 0,200) |
| `min_heap` của node | 9 452 B, trôi dần xuống | **33 284 B**, đứng yên |
| Ảnh nạp | 1 326 KB | 1 236 KB |
| Mất broker 60 giây | — | **0 bản ghi mất** (spool lên 1531 rồi rút về 6) |

Con số đầu là điểm chính. HTTP không cho máy chủ đẩy xuống, nên node phải
**đi hỏi** `GET /node/v1/commands` mỗi 2 giây; độ trễ trung bình của một lần
bấm nút chính là nửa chu kỳ đó, và nó không nhỏ đi được bằng cách tối ưu gì
cả — chỉ bằng cách đổi cách truyền.

Hai lý do còn lại, đo hôm 18/09: một edge ngốn **0,231 giây CPU mỗi giây**
của Odoo (bão hoà ở **5 edge** — nhà máy 10 edge thì Odoo đứng), và **289
lượt gọi HTTP mỗi 2 phút** mà phần lớn là hỏi rồi nhận "không có gì".

---

## Đường dây hai chiều

```
             fms/<serial>/meas      số đo, QoS 1
  node  ───► fms/<serial>/status    {"online":true,"cmd":true} retained
   ▲         fms/<serial>/cmdack    kết quả một lệnh
   │
   └─────◄── fms/<serial>/cmd       lệnh, QoS 1, KHÔNG retain
```

**Chiều lên** chạy từ *spooler* chứ không từ một cái tap: `spool_peek()` →
publish → `spool_ack_through()` **khi có PUBACK**. Ack theo PUBACK chứ không
theo `enqueue()`: enqueue chỉ nói "đã bỏ vào hàng của tác vụ mạng", còn
outbox của esp-mqtt nằm trong RAM và mất khi khởi động lại. Chỉ PUBACK mới là
lời hứa của broker rằng nó đã nhận. Đây là chỗ 2048 bản ghi đệm đến từ.

**Chiều xuống** không retain và không dùng phiên bền, **có chủ đích**: một
lệnh bật đèn gửi lúc node mất điện không được phép tự bật lên khi nó sống lại
nửa tiếng sau. Lệnh là thứ của hiện tại. Broker vứt đi lệnh gửi cho một node
vắng mặt, đúng như ta muốn.

**Node tự khai `"cmd":true`** trong chủ đề status. Edge đọc cờ đó để chọn
đường, nên firmware cũ (chỉ biết `GET /node/v1/commands`) vẫn được phục vụ
bằng hàng đợi poll mà không phải đặt cấu hình ở hai nơi rồi để chúng lệch
nhau.

**Báo khi đổi (report-by-exception)** lọc ~95% lưu lượng: ngày 18/09
`count1`/`pedal1`/`count2` phát số 0 mỗi 200 ms và chiếm 75% băng thông chỉ
để nói "vẫn là 0". Nhưng giá trị đứng yên **vẫn phải được nhắc lại** định kỳ
(`MAX_SILENCE_MS`), vì kênh bên Odoo có `max_age_ms` — im lâu hơn ngưỡng đó
thì ô giá trị chuyển xám và nút `[Đạt]` bị chặn.

---

## Trong repo có gì

```
components/mqtt_link/      component mới cho node, tự chứa
components/uplink/         chỉ Kconfig — công tắc bật/tắt HTTP
integration/*.patch        5 bản vá vào firmware đang có
broker/                    RabbitMQ chạy tại nhà máy (docker compose)
edge/                      3 file cho pcm-edge-collector
```

### Ghép vào firmware

```bash
cp -r <repo>/components/mqtt_link components/
cp <repo>/components/uplink/Kconfig.projbuild components/uplink/
for p in <repo>/integration/*.patch; do git apply "$p"; done
```

| bản vá | làm gì |
|--|--|
| `0001` | thêm **tap** vào `meas_core` (chỉ dùng khi HTTP còn bật) |
| `0002` | gọi `mqtt_link_start()` trong `main.c`, **và thêm `mqtt_link` vào `PRIV_REQUIRES`** |
| `0004` | bao `uplink.c` bằng `CONFIG_UPLINK_HTTP_ENABLE` |
| `0005` | bỏ `heapwatch`, thêm số đếm MQTT vào dòng status 10 giây |

> `0002` mà thiếu vế `PRIV_REQUIRES` thì build dừng với *"…however mqtt_link
> is not in the requirements list of main"*. ESP-IDF bắt khai báo phụ thuộc
> tường minh giữa các component, không tự suy từ `#include`.

### Cấu hình

```bash
idf.py menuconfig
#  →  FMS: duong HTTP (uplink)     →  [ ] Bat duong HTTP        ← bỏ chọn
#  →  FMS: duong MQTT (mqtt_link)  →  địa chỉ broker, user/pass
#  →  Component config → ESP-MQTT  →  [*] MQTT_USE_CUSTOM_CONFIG
#                                      MQTT poll read timeout = 100
```

Ba thứ **phải** đặt cùng nhau:

1. **`CONFIG_UPLINK_HTTP_ENABLE=n`** — bao cả `uplink.c`. `esp_http_client`,
   bộ đệm 4 KB và ngăn xếp 8 KB biến khỏi ảnh nạp; `uplink_server_ok()` trả
   về `mqtt_link_connected()` (nếu không thì đèn XANH báo sức khoẻ tắt vĩnh
   viễn), `uplink_kick()` và `uplink_sent_total()` chuyển sang MQTT.
2. **`MQTT_POLL_READ_TIMEOUT_MS=100`** — `esp_mqtt_client_enqueue()` **không**
   đánh thức tác vụ mạng, nó chờ vòng poll kế tiếp, mặc định **1000 ms**. Đo
   được đúng một luồng 1,0 giây trong độ trễ lệnh trước khi sửa. Tuỳ chọn này
   chỉ tồn tại khi `MQTT_USE_CUSTOM_CONFIG` bật — đặt riêng nó thì kconfgen
   lặng lẽ xoá đi và không có gì báo.
3. **`EDGE_MQTT_CONSUMER_FORWARD=true`** ở bên edge. Để `false` thì số đo
   chạy tới consumer rồi dừng lại — không còn đường nào khác tới Odoo.

> ⚠️ Mật khẩu broker đặt trong `sdkconfig` bị **nung vào firmware**. Đổi nó
> đòi hỏi nạp lại node, nên hãy gộp việc xoay mật khẩu vào lần nạp kế tiếp.
> Giai đoạn sau: chứng danh riêng từng thiết bị, nạp qua trang `/setup`, và
> `mqtts://`.

### Bên edge

Ba file, mount đè lên image (xem `edge/README.md`):

| file | thay đổi |
|--|--|
| `mqtt_consumer.py` | đọc `meas`/`status`/`cmdack`, đẩy lệnh xuống, nhịp tim thay mặt node, chặn dấu thời gian vô lý |
| `manager.py` | `queue_command()` đi MQTT trước, hàng đợi poll làm dự phòng |
| `scheduler.py` | nối `manager.mqtt_cmd = mqtt_consumer` |

Hai cái bẫy đã gặp thật, ghi lại để khỏi tìm lại:

- **Kho retained của RabbitMQ 4 không phục vụ đăng ký ký tự đại diện.**
  `fms/68EE8F4F06A8/status` nhận được bản retained; `fms/+/status` không nhận
  gì. Nên consumer đăng ký **đại diện** cho sự kiện sống, và đăng ký **chính
  xác** lần đầu thấy một serial để lấy ảnh chụp.
- **RabbitMQ 4 cấm hàng đợi tạm thời không độc quyền** (`--durable false` →
  `transient_nonexcl_queues is deprecated`).

---

## Kiểm chứng

```bash
# trên máy edge
curl -s localhost:8090/healthz | python3 -m json.tool
```

`forwarded` phải tăng đều, `bad` = 0, `ts_dropped` = 0, và `cmd_sent` ==
`cmd_acked`.

```
# trên node, dòng status mỗi 10 giây
status: heap=40356 min_heap=33284 rssi=-53 spool=3 sent=58 drop=0 \
        mq=1 pub=58 mqdrop=0 rbe=967
```

`spool` phải dao động quanh 0 (đang rút kịp), `drop`/`mqdrop` = 0, `mq` = 1.
`rbe` là số bản ghi bị nén lại — nó lớn hơn `pub` khoảng 20 lần, đó là phần
băng thông đã tiết kiệm.

**Đừng dùng `docker logs | grep -c`** để đếm: đo hôm 18/09 nó thiếu ~4 lần so
với bảng `history` trong SQLite của edge.

---

## Broker

Xem `broker/README.md`. Tóm tắt: RabbitMQ 4 + plugin MQTT, chạy **tại nhà
máy**, trên chính máy edge.

Đặt ở nhà máy chứ không đặt cạnh Odoo là chủ ý: **mất internet thì dây chuyền
vẫn phải chạy.** Broker ở xa thì đứt cáp là đèn tắt, nút bấm chết. Bài toán
nhiều nhà máy giải bằng liên kết hai broker (RabbitMQ shovel) — mỗi nhà máy
đẩy ra một broker trung tâm qua **một kết nối duy nhất đi ra**, không phải mở
cổng vào.

---

## Trạng thái

| giai đoạn | việc | tình trạng |
|--|--|--|
| 1 | dựng broker | ✅ |
| 2 | node phát song song với HTTP | ✅ |
| 2b | consumer đọc (chế độ bóng) | ✅ 2402 bản ghi/60 s, 0 gói hỏng |
| 3 | **cắt HTTP, lệnh đi MQTT** | ✅ **0,133 s một lệnh** |
| 4 | Last Will thay heartbeat của node | ✅ consumer dịch sang nhịp tim Odoo |
| 5 | cấu hình retained, bỏ hẳn `node_api` | ⏳ |
| 6 | đưa vùng chết xuống `meas_core` | ⏳ |

Giai đoạn 6 quan trọng hơn vẻ ngoài của nó, vì hai lý do.

Thứ nhất: ngày 18/09 một trigger không có vùng chết đã làm **sập instance
Odoo** — 2891 lần kích trong 6 giờ với trung vị bước thay đổi **0,0000 g**.
Bus nhanh hơn không chữa loại lỗi đó, nó chỉ chuyển lỗi đi nhanh hơn. Vùng
chết phải nằm ở chỗ phát.

Thứ hai: hôm nay vùng chết lọc ở **chỗ publish**, nên spooler vẫn chứa mẫu
thô — 2048 bản ghi ở nhịp 17 mẫu/giây chỉ đệm được **khoảng 2 phút** mất
broker. Đẩy vùng chết xuống `meas_core` thì cùng bộ nhớ đó đệm được hàng giờ.
