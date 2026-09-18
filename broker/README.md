# Broker tại nhà máy

RabbitMQ 4 + plugin MQTT, chạy trên chính máy edge.

## Dựng

```bash
mkdir -p ~/pcm_broker && cd ~/pcm_broker
# chép 3 file trong thư mục này vào đây, rồi:
printf 'BROKER_USER=pcm\nBROKER_PASS=%s\n' \
  "$(openssl rand -base64 24 | tr -d '/+=' | cut -c1-24)" > .env
chmod 600 .env
docker compose up -d
```

Để **riêng một project compose**, đừng gộp vào `docker-compose.yml` của
edge_collector: chạy `docker compose up -d` ở project đó sẽ recreate
edge_collector, mà `.env` của nó có biến chỉ đọc lúc tạo container.

## Cổng

| cổng | giao thức | mở tới |
|--|--|--|
| 1883 | MQTT | LAN — node nối vào đây |
| 5672 | AMQP | chỉ 127.0.0.1 — edge_collector cùng máy |
| 15672 | quản trị | chỉ 127.0.0.1 — xem qua SSH tunnel |

Xem trang quản trị từ máy khác:

```bash
ssh -L 15672:127.0.0.1:15672 <user>@<máy edge>
# rồi mở http://127.0.0.1:15672
```

## Kiểm broker sống

```bash
. ./.env
A="docker exec pcm_broker-broker-1 rabbitmqadmin --non-interactive -V /pcm -u $BROKER_USER -p $BROKER_PASS"

$A declare queue --name test.q --type classic --durable true
$A declare binding --source amq.topic --destination-type queue \
   --destination test.q --routing-key 'fms.#'

docker run --rm --network host eclipse-mosquitto:2 mosquitto_pub \
  -h 127.0.0.1 -p 1883 -u $BROKER_USER -P $BROKER_PASS -q 1 \
  -t 'fms/TEST/meas' -m '{"ch":"scale_esp32","v":56.43}'

sleep 5
$A get messages --queue test.q --count 5 --ack-mode reject_requeue_true
$A delete queue --name test.q
```

Thấy thông điệp với khoá định tuyến `fms.TEST.meas` là **đường MQTT → AMQP
thông**. Đó là điều kiện duy nhất của giai đoạn 1.

Lưu ý RabbitMQ 4 **cấm queue không bền** (`--durable false` sẽ lỗi
`transient_nonexcl_queues is deprecated`).

## Giới hạn tài nguyên

Máy edge còn phải nuôi edge_collector và mysql, nên broker bị ghim:
`vm_memory_high_watermark 768 MB`, trần container 1 GB, chặn đĩa 2 GB. Vượt
ngưỡng thì broker chặn bên phát lại, không làm sập máy. Đo thực tế lúc rảnh:
**124 MB**.
