# Consumer — bên đọc của đường MQTT

Dành cho `pcm-edge-collector`. Node đã phát lên `fms/<serial>/meas`, nhưng
**một topic exchange không có ai đăng ký thì broker vứt thông điệp đi**. Đây
là nửa còn lại của đường ống.

```
node --MQTT--> broker --MQTT--> mqtt_consumer.py --> agent.push_node_reading()
                                                            |
                                                (đúng cửa mà node_api.py dùng)
                                                            |
                                                  outbox SQLite --> Odoo
```

Dùng **đúng cửa vào `push_node_reading()`** của đường HTTP, nên không có bộ
phân tích thứ hai, không có đường ghi thứ hai vào outbox, và mọi thứ phía sau
(chống mất mẫu, gom lô, gửi lại khi đứt mạng) giữ y nguyên.

---

## ⚠️ Chế độ bóng — đọc kỹ chỗ này

Ở giai đoạn 2, node gửi **cùng một số đo bằng cả hai đường**: HTTP tới
`/node/v1/measurements` và MQTT tới đây. Nếu consumer cũng đẩy vào Odoo thì
**Odoo nhận mỗi mẫu hai lần** — sai dữ liệu và gấp đôi tải ghi.

Nên mặc định nó **chỉ đếm, không đẩy**. Đếm để đối chiếu:

```
số mẫu qua MQTT  ==  số mẫu qua HTTP   →  đường ống sạch, đủ điều kiện cắt HTTP
```

Chỉ bật `EDGE_MQTT_CONSUMER_FORWARD=true` **sau khi đã cắt đường HTTP của
node**, không bao giờ bật lúc cả hai đang chạy.

---

## Cài

```bash
cp <repo>/edge/mqtt_consumer.py edge_collector/
git apply <repo>/edge/0003-wire-mqtt-consumer.patch
```

Bản vá chạm ba tệp, tổng 43 dòng thêm:

| tệp | việc |
|--|--|
| `config.py` | 8 thiết lập mới + helper `_bool` + khoá vào `RESTART_REQUIRED_KEYS` |
| `scheduler.py` | `EdgeAgent` tạo · khởi động · dừng consumer |
| `app.py` | đưa số đếm ra `/healthz` |

Không thêm phụ thuộc nào: `paho-mqtt==1.6.1` đã có sẵn trong
`requirements.txt`.

## Cấu hình (`.env`)

```ini
EDGE_MQTT_CONSUMER=true
EDGE_MQTT_CONSUMER_URL=mqtt://127.0.0.1:1883
EDGE_MQTT_CONSUMER_USER=pcm
EDGE_MQTT_CONSUMER_PASS=<xem ~/pcm_broker/.env>
EDGE_MQTT_CONSUMER_FORWARD=false      # giai đoạn 2: để false
```

Các khoá này **không hot-reload được** — paho bind phiên lúc khởi động, nên
đổi xong phải khởi động lại edge_collector. Chúng đã nằm trong
`RESTART_REQUIRED_KEYS` để trang `/setup` hiện đúng cảnh báo.

## Xem kết quả

```bash
curl -s localhost:8090/healthz | jq .mqtt_consumer
```

```json
{
  "connected": true,
  "messages": 284,
  "items": 2402,
  "forwarded": 0,
  "bad": 0,
  "by_serial": {"68EE8F4F06A8": 2402},
  "online": {"68EE8F4F06A8": true}
}
```

`items` là con số đem đối chiếu với đường HTTP. `forwarded` phải bằng **0**
suốt giai đoạn 2.

---

## Hai điều đo được trên broker thật, không phải suy đoán

### Kho retained của RabbitMQ không phục vụ ký tự đại diện

Đo 18/09 trên chính broker này:

```
đăng ký 'fms/68EE8F4F06A8/status'  →  {"online":true}   ✅
đăng ký 'fms/+/status'             →  (không gì cả)     ❌
```

Đăng ký đại diện vẫn bắt được **thay đổi sống** (Last Will, lần node báo
online) — chỉ thiếu **ảnh chụp retained** lúc consumer vừa khởi động.

Nên consumer làm cả hai: đại diện cho sự kiện sống, và khi lần đầu thấy một
serial trong `meas` thì đăng ký thêm chủ đề status **chính xác** của serial
đó để lấy ảnh chụp.

Điều này cũng có nghĩa **giai đoạn 5 vẫn ổn**: node đăng ký chủ đề `cfg` của
chính nó, không dùng ký tự đại diện.

### Phiên bền bỉ giữ hộ thông điệp thật

Consumer dùng `clean_session=False` + `client_id` cố định + QoS 1. Đo được:
tốc độ thật ~16,6 bản ghi/giây, nhưng lần chạy sau khi consumer vắng mặt vài
phút nhận **2402 bản ghi trong 60 giây** — phần dôi ra là broker trả lại số
đã giữ hộ.

Đây chính là thứ HTTP không làm được, và là lý do `client_id` **phải cố định
và phải khác nhau giữa các tiến trình**: hai bên dùng chung một `client_id`
sẽ đá nhau ra khỏi broker liên tục.
