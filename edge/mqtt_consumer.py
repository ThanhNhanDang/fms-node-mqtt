# -*- coding: utf-8 -*-
"""Ben DOC cua duong MQTT: nhan so do node phat len broker.

Vi sao co file nay: node (component mqtt_link) da phat len
`fms/<serial>/meas`, nhung mot topic exchange khong co ai dang ky thi broker
VUT thong diep di. Do la trang thai truoc khi co file nay — nua duong ong.

Duong di:

    node --MQTT--> broker --MQTT--> file nay --> agent.push_node_reading()
                                                        |
                                            (dung cua ma node_api.py dung)
                                                        |
                                              outbox SQLite --> Odoo

Dung DUNG cua vao `push_node_reading()` cua duong HTTP, nen khong co bo
phan tich thu hai, khong co duong ghi thu hai vao outbox, va moi thu phia
sau (chong mat mau, gom lo, gui lai khi dut mang) dung y nguyen.


CHE DO BONG (mac dinh) — doc ky cho nay
---------------------------------------
O giai doan 2, node gui CUNG MOT so do bang CA HAI duong: HTTP toi
/node/v1/measurements va MQTT toi day. Neu file nay cung day vao Odoo thi
Odoo nhan MOI mau HAI LAN — sai du lieu va gap doi tai ghi.

Nen mac dinh no chi DEM, khong day. Dem de doi chieu:

    so mau qua MQTT  ==  so mau qua HTTP   -> duong ong sach, du dieu kien
                                              cat HTTP o giai doan sau

Bat `EDGE_MQTT_CONSUMER_FORWARD=true` CHI KHI da cat duong HTTP cua node,
khong bao gio bat luc ca hai dang chay.


Ben bi khi consumer chet
------------------------
Dung phien MQTT ben bi: `clean_session=False` + client_id co dinh + dang ky
QoS 1. Broker giu thong diep lai cho dung client_id do trong luc no vang
mat (RabbitMQ: `mqtt.max_session_expiry_interval_seconds`, dang dat 3600 s).
Song lai la doc tiep tu cho dut, khong mat mau.

Day la ly do client_id PHAI co dinh va PHAI khac nhau giua cac tien trinh:
hai ben dung chung mot client_id se da nhau ra khoi broker lien tuc.
"""
import asyncio
import json
import logging
import time

import paho.mqtt.client as mqtt

from .config import settings

_logger = logging.getLogger("edge.mqtt_consumer")

# Gioi han mot goi — node gui toi da 50 ban ghi mot lo (CONFIG_MQTT_LINK_
# BATCH_MAX), lay du gap boi de con cho firmware khac, nhung van chan mot
# goi hong/ac y lam nghen vong lap.
MAX_ITEMS = 1000


class MqttConsumer:
    """Doc `fms/<serial>/meas` va `fms/<serial>/status` tu broker."""

    def __init__(self, agent):
        self._agent = agent
        self._cli = None
        self._loop = None
        self._connected = False
        self.stats = {
            "connected": False,
            "messages": 0,      # so goi MQTT nhan duoc
            "items": 0,         # so BAN GHI so do — con so de doi chieu voi HTTP
            "forwarded": 0,     # so ban ghi thuc su day vao outbox
            "bad": 0,           # goi khong phan tich duoc
            "last_ts": None,
            "by_serial": {},    # serial -> so ban ghi
            "online": {},       # serial -> True/False theo chu de status
        }

    # -- vong doi ------------------------------------------------------
    async def start(self) -> None:
        if not settings.mqtt_consumer_enabled:
            _logger.info("tat trong cau hinh, khong chay")
            return
        self._loop = asyncio.get_event_loop()

        host, port = _split_url(settings.mqtt_consumer_url)
        # clean_session=False: xem phan "Ben bi" o dau file.
        self._cli = mqtt.Client(client_id=settings.mqtt_consumer_client_id,
                                clean_session=False)
        if settings.mqtt_consumer_user:
            self._cli.username_pw_set(settings.mqtt_consumer_user,
                                      settings.mqtt_consumer_pass or "")
        self._cli.on_connect = self._on_connect
        self._cli.on_message = self._on_message
        self._cli.on_disconnect = self._on_disconnect
        self._cli.connect_async(host, port, keepalive=30)
        self._cli.loop_start()
        _logger.info("dang noi broker %s:%s, chu de %s (che do %s)",
                     host, port, settings.mqtt_consumer_topic,
                     "DAY VAO ODOO" if settings.mqtt_consumer_forward else "bong/chi dem")

    async def stop(self) -> None:
        if self._cli:
            self._cli.loop_stop()
            self._cli.disconnect()
            self._cli = None

    # -- callback cua paho (chay tren THREAD RIENG) ---------------------
    #
    # Khong dung thang vao store/agent o day: chuyen ve vong lap asyncio
    # bang call_soon_threadsafe, giong drivers/mqtt.py. Giu dung thu tu va
    # khong de I/O SQLite chan vong mang cua paho.
    def _on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            client.subscribe([(settings.mqtt_consumer_topic, 1),
                              (settings.mqtt_consumer_status_topic, 1)])
            self._loop.call_soon_threadsafe(self._set_connected, True)
        else:
            self._loop.call_soon_threadsafe(
                self._log_error, "noi broker that bai rc=%s" % rc)

    def _on_disconnect(self, client, userdata, rc):
        self._loop.call_soon_threadsafe(self._set_connected, False)

    def _on_message(self, client, userdata, msg):
        self._loop.call_soon_threadsafe(self._handle, msg.topic, msg.payload)

    # -- chay tren vong lap asyncio -------------------------------------
    def _set_connected(self, ok: bool):
        self._connected = ok
        self.stats["connected"] = ok
        _logger.info("broker: %s", "da noi" if ok else "mat ket noi")

    def _log_error(self, text: str):
        _logger.warning("%s", text)

    def _subscribe_status(self, serial: str) -> None:
        """Dang ky chu de status chinh xac cua mot serial (lay anh chup retained)."""
        if not self._cli:
            return
        topic = settings.mqtt_consumer_status_topic.replace("+", serial, 1)
        if "+" in topic or "#" in topic:
            return          # mau chu de khong co cho de thay serial vao
        try:
            self._cli.subscribe(topic, 1)
            _logger.info("dang ky them %s", topic)
        except Exception as exc:                                  # noqa: BLE001
            _logger.warning("khong dang ky duoc %s: %s", topic, exc)

    def _handle(self, topic: str, raw: bytes):
        serial, kind = _parse_topic(topic)
        if not serial:
            return
        try:
            data = json.loads(raw.decode("utf-8", errors="replace"))
        except ValueError:
            self.stats["bad"] += 1
            return
        if not isinstance(data, dict):
            self.stats["bad"] += 1
            return

        if kind == "status":
            self.stats["online"][serial] = bool(data.get("online"))
            _logger.info("node %s: %s", serial,
                         "online" if data.get("online") else "OFFLINE (Last Will)")
            return

        items = data.get("items")
        if not isinstance(items, list):
            self.stats["bad"] += 1
            return

        # Lan dau thay mot serial: dang ky them chu de status CHINH XAC cua no.
        #
        # Vi sao phai lam the, thay vi tin vao 'fms/+/status' da dang ky o
        # _on_connect: kho retained cua RabbitMQ KHONG phuc vu dang ky co ky
        # tu dai dien. Do that 18/09 tren chinh broker nay:
        #     'fms/68EE8F4F06A8/status' -> nhan duoc {"online":true}
        #     'fms/+/status'            -> khong nhan gi
        # Dang ky dai dien van bat duoc thay doi SONG (Last Will, lan node
        # bao online), chi thieu anh chup retained luc minh vua khoi dong.
        # Nen: dai dien cho su kien song + chinh xac cho anh chup.
        if serial not in self.stats["by_serial"]:
            self._subscribe_status(serial)

        n = 0
        for it in items[:MAX_ITEMS]:
            if not isinstance(it, dict):
                continue
            ch = it.get("ch")
            if not ch:
                continue
            n += 1
            if not settings.mqtt_consumer_forward:
                continue
            ts = it.get("ts")
            self._agent.push_node_reading(
                serial, ch, it.get("v"), it.get("s"), int(it.get("q") or 0),
                (ts / 1000.0) if isinstance(ts, (int, float)) else None,
                it.get("stable"),
            )
            self.stats["forwarded"] += 1

        self.stats["messages"] += 1
        self.stats["items"] += n
        self.stats["last_ts"] = time.time()
        self.stats["by_serial"][serial] = self.stats["by_serial"].get(serial, 0) + n


def _split_url(url: str):
    """'mqtt://host:port' | 'host:port' | 'host' -> (host, port)."""
    raw = (url or "").strip()
    for prefix in ("mqtt://", "tcp://"):
        if raw.startswith(prefix):
            raw = raw[len(prefix):]
    host, _, port = raw.partition(":")
    try:
        return (host or "127.0.0.1"), int(port or 1883)
    except ValueError:
        return (host or "127.0.0.1"), 1883


def _parse_topic(topic: str):
    """'fms/68EE8F4F06A8/meas' -> ('68EE8F4F06A8', 'meas')."""
    parts = (topic or "").split("/")
    if len(parts) < 3:
        return None, None
    return parts[-2], parts[-1]
