# fms-node-mqtt

Đưa node FMS (ESP32-S3) lên MQTT, **giai đoạn 2**: phát số đo song song với
đường HTTP đang chạy. Chưa cắt gì cả.

Dành cho firmware `fms-node` (ESP-IDF, target `esp32s3`) đang nói chuyện với
`pcm-edge-collector` qua `/node/v1/*`.

---

## Vì sao đổi

Ba con số đo trên hệ thật (RL-100, node `68EE8F4F06A8`, 18/09/2026):

| | hôm nay | vì sao |
|--|--|--|
| Độ trễ một lệnh điều khiển | **2,2 – 2,6 giây** | node phải *đi hỏi* `GET /node/v1/commands`; HTTP không cho máy chủ đẩy xuống |
| Một edge ngốn của Odoo | **0,231 giây CPU mỗi giây** | bão hòa ở **5 edge**. Nhà máy 10 edge thì Odoo đứng |
| Lượt gọi HTTP mỗi 2 phút | **289** | heartbeat, print_jobs, config, channels… phần lớn là hỏi rồi nhận "không có gì" |

MQTT không làm gói tin đi nhanh hơn. Nó giải quyết ba thứ khác:

1. **Máy chủ đẩy xuống được** — hết 2,2 giây chờ vòng hỏi.
2. **Thêm nhà máy không thêm luồng request** — chi phí không tăng tuyến tính.
3. **Last Will** — node rớt là broker báo ngay, không tốn gói heartbeat nào.

---

## Trong repo có gì

```
components/mqtt_link/      component mới, tự chứa
integration/*.patch        2 bản vá nhỏ vào file có sẵn
broker/                    RabbitMQ chạy tại nhà máy (docker compose)
```

**Không sửa `uplink.c`.** Đường HTTP chạy y nguyên, vẫn là đường chính thức,
vẫn là bên duy nhất gọi `spool_ack_through()`.

---

## Ghép vào firmware

Đứng ở thư mục gốc dự án `fms-node`:

```bash
# 1. component mới
cp -r <repo>/components/mqtt_link components/

# 2. hai bản vá
git apply <repo>/integration/0001-meas_core-tap.patch
git apply <repo>/integration/0002-main-start-mqtt_link.patch
```

Không dùng git thì `patch -p1 < ...`, hoặc sửa tay — mỗi bản vá chỉ vài dòng,
mở ra đọc là thấy.

### Hai bản vá đó làm gì

`0001` thêm vào `meas_core` một cái **tap**: hàm quan sát được gọi ngay trong
`meas_push`, sau khi `seq` và `ts` đã gán.

> **Vì sao cần tap mà không đọc thẳng spooler:** `spool_peek()` chỉ trả về
> những bản ghi **cũ nhất**, và `uplink` mới là bên xoá chúng đi bằng
> `spool_ack_through()`. Nếu MQTT cũng đọc ở đó thì hai đường tranh nhau một
> vòng đệm — HTTP ack trước là MQTT mất bản ghi, và số liệu đối chiếu của
> giai đoạn 2 thành vô nghĩa.

`0002` gọi `mqtt_link_start()` trong `main.c`, ngay sau `uplink_start()`.

---

## Cấu hình

```bash
idf.py menuconfig      #  →  FMS: duong MQTT (mqtt_link)
```

| mục | ví dụ |
|--|--|
| Địa chỉ broker | `mqtt://192.168.5.190:1883` |
| Tên đăng nhập / mật khẩu | theo `broker/.env` trên máy edge |
| Gốc chủ đề | `fms` |
| Số bản ghi mỗi gói | `50` |
| Thời gian gom | `500` ms |

**Để trống địa chỉ broker thì component không chạy** và node hoạt động y như
trước. Đó là cách tắt an toàn nhất.

> ⚠️ **Mật khẩu đặt ở đây bị nung vào firmware và nằm trong `sdkconfig`.**
> Chấp nhận được khi thử nghiệm. Giai đoạn 4 sẽ chuyển sang chứng danh riêng
> từng thiết bị, nạp qua trang `/setup` của node, và dùng `mqtts://`.

Chủ đề node sẽ phát:

```
fms/<serial>/meas      số đo, QoS 1
fms/<serial>/status    {"online":true} retained · Last Will {"online":false}
```

RabbitMQ đổi `/` thành `.`, nên bên AMQP nhận được khoá định tuyến
`fms.<serial>.meas` trên exchange `amq.topic`.

---

## Kiểm chứng giai đoạn 2

Mục đích của giai đoạn này là **đối chiếu**, không phải chuyển đổi. Hai đếm số
phải bám sát nhau:

```c
uplink_sent_total()      // HTTP đã gửi
mqtt_link_published()    // MQTT đã gửi
mqtt_link_dropped()      // MQTT bỏ (hàng đợi đầy / mất broker)
```

Cách xem nhanh, trên máy edge:

```bash
# đếm thông điệp MQTT tới
docker run --rm --network host eclipse-mosquitto:2 mosquitto_sub \
  -h 127.0.0.1 -p 1883 -u <user> -P <pass> -t 'fms/#' -v
```

**Đạt** khi sau 24 giờ số mẫu hai đường khớp nhau và `mqtt_link_dropped()`
bằng 0. Chưa đạt thì **không cắt HTTP** — cả kiến trúc sau này dựa vào việc
giai đoạn này sạch.

---

## Giai đoạn này KHÔNG làm gì

- Không nhận lệnh qua MQTT (đó là giai đoạn 3 — chỗ lấy lại 2,2 giây)
- Không bỏ heartbeat (giai đoạn 4)
- Không bỏ `/node/v1/config` (giai đoạn 5)
- Không có TLS, chưa có chứng danh từng thiết bị
- Không đệm khi mất broker — mất kết nối thì MQTT bỏ mẫu và đếm lại;
  **HTTP vẫn gửi đủ**, nên không mất dữ liệu thật

---

## Broker

Xem `broker/README.md`. Tóm tắt: RabbitMQ 4 + plugin MQTT, chạy **tại nhà
máy** trên chính máy edge.

Đặt ở nhà máy chứ không đặt cạnh Odoo là chủ ý: **mất internet thì dây chuyền
vẫn phải chạy.** Broker ở xa thì đứt cáp là đèn tắt, nút bấm chết. Bài toán
nhiều nhà máy giải bằng liên kết hai broker (RabbitMQ shovel) — mỗi nhà máy
đẩy ra một broker trung tâm qua **một kết nối duy nhất đi ra**, không phải mở
cổng vào.

---

## Trạng thái

| giai đoạn | việc | tình trạng |
|--|--|--|
| 1 | dựng broker, chưa ai dùng | ✅ xong, đã kiểm MQTT → AMQP |
| 2 | node phát song song | 📦 **repo này** — chưa nạp lên node |
| 3 | cắt đường lệnh sang MQTT | ⏳ |
| 4 | Last Will thay heartbeat | ⏳ |
| 5 | cấu hình retained, bỏ `node_api` | ⏳ |
| 6 | đưa vùng chết xuống `meas_core` | ⏳ |

Giai đoạn 6 quan trọng hơn vẻ ngoài của nó: ngày 18/09 một trigger không có
vùng chết đã làm sập instance Odoo — 2891 lần kích trong 6 giờ với **trung vị
bước thay đổi 0,0000 g**. Bus nhanh hơn không chữa loại lỗi đó, nó chỉ chuyển
lỗi đi nhanh hơn. Vùng chết phải nằm ở chỗ phát.
