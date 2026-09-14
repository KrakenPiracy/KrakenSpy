
import base64
import hashlib
from pathlib import Path
import json
import os
import sys
import queue
import ssl
import threading
import time
import uuid

import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet, InvalidToken
from PySide6.QtCore import QTimer, Qt, QUrl
from PySide6.QtMultimedia import QAudioOutput, QMediaPlayer
from PySide6.QtGui import QFont, QIcon
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QMessageBox,
    QPushButton,
    QVBoxLayout,
    QWidget,
    QTextBrowser,
)

APP_NAME = "KrakenSpy"
BROKER_HOST = "broker.emqx.io"
BROKER_KEEPALIVE = 45
BROKER_USERNAME = "emqx"
BROKER_PASSWORD = "public"

# Try several public MQTT transports automatically. Some networks/ISPs/firewalls
# block raw MQTT TCP, while WebSocket or TLS remains available.
RELAY_ENDPOINTS = [
    ("MQTT TCP", 1883, False, None),
    ("MQTT TLS", 8883, True, None),
    ("MQTT WebSocket", 8083, False, "/mqtt"),
    ("MQTT Secure WebSocket", 8084, True, "/mqtt"),
]
READ_DELETE_SECONDS = 30

# Shared only through the public code-derived topic. Message contents are separately
# encrypted with the same normalized room code.
TOPIC_PREFIX = "blinkchat/v5/rooms/"



def resource_path(filename: str) -> str:
    base = getattr(sys, "_MEIPASS", os.path.dirname(os.path.abspath(__file__)))
    return os.path.join(base, filename)


def normalize_code(value: str) -> str:
    return "".join(value.strip().upper().split())[:32]


def topic_for(code: str) -> str:
    return TOPIC_PREFIX + hashlib.sha256(normalize_code(code).encode()).hexdigest()[:32]


def key_for(code: str) -> Fernet:
    digest = hashlib.sha256(normalize_code(code).encode()).digest()
    return Fernet(base64.urlsafe_b64encode(digest))


def new_client_id() -> str:
    return "blinkchat-" + uuid.uuid4().hex


class Relay:
    """
    Network-only worker.

    IMPORTANT: MQTT callbacks never touch Qt. They only put events into a normal
    thread-safe queue. The QWidget polls that queue on the GUI thread.
    """

    def __init__(self, event_queue):
        self.events = event_queue
        self.client = None
        self.thread = None
        self.connected = threading.Event()
        self.stop_event = threading.Event()
        self.lock = threading.Lock()
        self.current_endpoint = None

    def start(self):
        self.thread = threading.Thread(target=self._connect_loop, daemon=True)
        self.thread.start()

    def _new_client(self, label, port, tls, ws_path):
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=new_client_id(),
            protocol=mqtt.MQTTv5,
            transport="websockets" if ws_path else "tcp",
        )

        client.username_pw_set(BROKER_USERNAME, BROKER_PASSWORD)

        if ws_path:
            client.ws_set_options(path=ws_path)

        if tls:
            client.tls_set(cert_reqs=ssl.CERT_REQUIRED)

        client.on_connect = self._on_connect
        client.on_disconnect = self._on_disconnect
        client.on_message = self._on_message
        client._blink_endpoint_label = label
        return client

    def _connect_loop(self):
        last_error = ""
        while not self.stop_event.is_set():
            # Once connected, wait here. A disconnect sends us back through the
            # endpoint list so we can recover automatically.
            for label, port, tls, ws_path in RELAY_ENDPOINTS:
                if self.stop_event.is_set():
                    return

                try:
                    self.events.put(("status", f"Trying {label} ({port})…"))
                    client = self._new_client(label, port, tls, ws_path)

                    with self.lock:
                        old = self.client
                        self.client = client

                    if old is not None:
                        try:
                            old.loop_stop()
                            old.disconnect()
                        except Exception:
                            pass

                    client.connect(BROKER_HOST, port, BROKER_KEEPALIVE)
                    client.loop_start()

                    # Wait up to 7 seconds for MQTT CONNACK.
                    if self.connected.wait(7):
                        self.current_endpoint = (label, port)
                        while (
                            not self.stop_event.is_set()
                            and self.connected.is_set()
                        ):
                            self.stop_event.wait(0.5)
                        try:
                            client.loop_stop()
                            client.disconnect()
                        except Exception:
                            pass
                        self.connected.clear()
                        if not self.stop_event.is_set():
                            self.events.put(("status", "Relay disconnected — reconnecting…"))
                        time.sleep(0.5)
                        break

                    # Connection did not establish.
                    self.connected.clear()
                    try:
                        client.loop_stop()
                        client.disconnect()
                    except Exception:
                        pass
                    last_error = f"{label}: no connection"
                    self.events.put(("status", f"{label} failed"))
                    time.sleep(0.4)

                except Exception as exc:
                    last_error = f"{label}: {type(exc).__name__}: {exc}"
                    self.connected.clear()
                    try:
                        client.loop_stop()
                        client.disconnect()
                    except Exception:
                        pass
                    self.events.put(("status", f"{label} failed: {type(exc).__name__}"))
                    time.sleep(0.4)

            if not self.stop_event.is_set():
                self.events.put(("status", "All relay methods failed — retrying in 3s"))
                self.events.put(("detail", last_error))
                self.stop_event.wait(3)

    def _on_connect(self, client, userdata, flags, reason_code, properties):
        # Paho MQTT v5 exposes ReasonCode objects whose string representation can
        # be "Success". Comparing the object directly avoids misclassifying a
        # successful connection as a failure.
        try:
            success = (reason_code == 0) or (str(reason_code).lower() == "success")
        except Exception:
            success = False

        if success:
            self.connected.set()
            self.events.put(("status", f"Connected • {client._blink_endpoint_label}"))
        else:
            self.connected.clear()
            self.events.put(("status", f"Broker refused connection ({reason_code})"))

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        self.connected.clear()

    def _on_message(self, client, userdata, msg):
        # This is the only thing the MQTT callback does with application data.
        self.events.put(("message", bytes(msg.payload)))

    def subscribe(self, topic):
        def worker():
            if not self.connected.wait(15):
                self.events.put(("status", "Still trying to connect to relay…"))
                return
            try:
                with self.lock:
                    client = self.client
                if client:
                    rc, _mid = client.subscribe(topic, qos=1)
                    if rc != mqtt.MQTT_ERR_SUCCESS:
                        self.events.put(("status", f"Subscribe failed ({rc})"))
            except Exception as exc:
                self.events.put(("status", f"Subscribe error: {exc}"))

        threading.Thread(target=worker, daemon=True).start()

    def publish(self, topic, payload: bytes):
        def worker():
            if not self.connected.wait(15):
                self.events.put(("status", "Still connecting — message not sent"))
                return
            try:
                with self.lock:
                    client = self.client
                if client:
                    info = client.publish(topic, payload=payload, qos=1, retain=False)
                    if info.rc != mqtt.MQTT_ERR_SUCCESS:
                        self.events.put(("status", f"Send failed ({info.rc})"))
            except Exception as exc:
                self.events.put(("status", f"Send error: {exc}"))

        threading.Thread(target=worker, daemon=True).start()

    def stop(self):
        self.stop_event.set()
        with self.lock:
            client = self.client
        try:
            if client:
                client.loop_stop()
                client.disconnect()
        except Exception:
            pass


class SoundPlayer:
    def __init__(self):
        self.sent_output = QAudioOutput()
        self.sent_output.setVolume(0.45)
        self.sent_player = QMediaPlayer()
        self.sent_player.setAudioOutput(self.sent_output)

        self.recv_output = QAudioOutput()
        self.recv_output.setVolume(0.5)
        self.recv_player = QMediaPlayer()
        self.recv_player.setAudioOutput(self.recv_output)

        self.sent_sound = resource_path("Sent.mp3")
        self.receive_sound = resource_path("Receive.mp3")

    def _play(self, player, path):
        try:
            player.stop()
            player.setSource(QUrl.fromLocalFile(str(path)))
            player.play()
        except Exception:
            pass

    def sent(self):
        self._play(self.sent_player, self.sent_sound)

    def received(self):
        self._play(self.recv_player, self.receive_sound)


class BlinkChat(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setWindowIcon(QIcon(resource_path("KrakenSpy.ico")))
        self.resize(820, 650)
        self.sound = SoundPlayer()
        self.sound.sent_sound = str(Path(__file__).with_name("Sent.mp3"))
        self.sound.receive_sound = str(Path(__file__).with_name("Receive.mp3"))

        self.nickname = ""
        self.code = ""
        self.topic = ""
        self.cipher = None
        self.room_active = False
        self.visible_messages = []
        self.events = queue.Queue()
        self.relay = Relay(self.events)
        self.relay.start()

        self.build_home()
        self.build_chat()

        self.poll_timer = QTimer(self)
        self.poll_timer.timeout.connect(self.poll_events)
        self.poll_timer.start(50)

        self.expire_timer = QTimer(self)
        self.expire_timer.timeout.connect(self.expire_messages)
        self.expire_timer.start(200)

        self.advertise_timer = QTimer(self)
        self.advertise_timer.timeout.connect(self.publish_presence)

    def build_home(self):
        self.home = QWidget()
        layout = QVBoxLayout(self.home)
        layout.setContentsMargins(42, 36, 42, 30)
        layout.setSpacing(14)

        brand = QHBoxLayout()
        brand.setSpacing(14)
        logo = QLabel()
        logo.setObjectName("logoMark")
        logo.setFixedSize(54, 54)
        logo.setAlignment(Qt.AlignmentFlag.AlignCenter)
        icon = QIcon(resource_path("KrakenSpy.ico"))
        if not icon.isNull():
            logo.setPixmap(icon.pixmap(46, 46))
        brand.addWidget(logo)

        title_col = QVBoxLayout()
        title = QLabel("KRAKENSPY")
        title.setObjectName("brandTitle")
        title.setFont(QFont("Consolas", 27, QFont.Weight.Bold))
        subtitle = QLabel("PRIVATE // EPHEMERAL // OPEN SOURCE")
        subtitle.setObjectName("brandSub")
        title_col.addWidget(title)
        title_col.addWidget(subtitle)
        brand.addLayout(title_col)
        brand.addStretch()
        layout.addLayout(brand)

        status_row = QHBoxLayout()
        self.relay_dot = QLabel("●")
        self.relay_dot.setObjectName("statusDot")
        self.relay_status = QLabel("RELAY: INITIALIZING")
        self.relay_status.setObjectName("statusText")
        status_row.addWidget(self.relay_dot)
        status_row.addWidget(self.relay_status)
        status_row.addStretch()
        self.clock_label = QLabel("--:--:--")
        self.clock_label.setObjectName("clock")
        status_row.addWidget(self.clock_label)
        layout.addLayout(status_row)

        line = QFrame()
        line.setObjectName("accentLine")
        line.setFixedHeight(2)
        layout.addWidget(line)
        layout.addSpacing(8)

        identity = QFrame()
        identity.setObjectName("panel")
        il = QVBoxLayout(identity)
        il.setContentsMargins(18, 16, 18, 16)
        il.addWidget(QLabel("IDENTITY", objectName="sectionLabel"))
        self.name_input = QLineEdit()
        self.name_input.setPlaceholderText("ENTER CODENAME")
        il.addWidget(self.name_input)
        layout.addWidget(identity)

        cards = QHBoxLayout()
        cards.setSpacing(14)

        create_frame = QFrame()
        create_frame.setObjectName("panel")
        cf = QVBoxLayout(create_frame)
        cf.setContentsMargins(18, 18, 18, 18)
        cf.addWidget(QLabel("CREATE CHANNEL", objectName="sectionLabel"))
        self.create_code = QLineEdit()
        self.create_code.setPlaceholderText("ENTER ROOM CODE")
        self.create_code.returnPressed.connect(self.create_chat)
        cf.addWidget(self.create_code)
        create_btn = QPushButton("INITIALIZE CHANNEL")
        create_btn.setObjectName("primaryButton")
        create_btn.clicked.connect(self.create_chat)
        cf.addWidget(create_btn)
        hint = QLabel("Become the channel origin.")
        hint.setObjectName("dimText")
        cf.addWidget(hint)
        cards.addWidget(create_frame, 1)

        join_frame = QFrame()
        join_frame.setObjectName("panel")
        jf = QVBoxLayout(join_frame)
        jf.setContentsMargins(18, 18, 18, 18)
        jf.addWidget(QLabel("JOIN CHANNEL", objectName="sectionLabel"))
        self.join_code = QLineEdit()
        self.join_code.setPlaceholderText("ENTER ROOM CODE")
        self.join_code.returnPressed.connect(self.join_chat)
        jf.addWidget(self.join_code)
        join_btn = QPushButton("ACCESS CHANNEL")
        join_btn.setObjectName("secondaryButton")
        join_btn.clicked.connect(self.join_chat)
        jf.addWidget(join_btn)
        hint2 = QLabel("Enter the same room code.")
        hint2.setObjectName("dimText")
        jf.addWidget(hint2)
        cards.addWidget(join_frame, 1)

        layout.addLayout(cards)

        console = QFrame()
        console.setObjectName("console")
        cl = QVBoxLayout(console)
        cl.setContentsMargins(14, 10, 14, 10)
        self.boot_console = QLabel("> krakenspy core loaded\n> secure ephemeral mode: READY\n> waiting for channel command…")
        self.boot_console.setObjectName("consoleText")
        self.boot_console.setWordWrap(True)
        cl.addWidget(self.boot_console)
        layout.addWidget(console)

        footer = QLabel("NO LOCAL MESSAGE DATABASE  •  30s EPHEMERAL DISPLAY  •  ENCRYPTED PAYLOADS")
        footer.setObjectName("footer")
        footer.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addStretch()
        layout.addWidget(footer)

    def build_chat(self):
        self.chat = QWidget()
        layout = QVBoxLayout(self.chat)
        layout.setContentsMargins(26, 22, 26, 22)
        layout.setSpacing(12)

        top = QHBoxLayout()
        back = QPushButton("← HUB")
        back.setObjectName("ghostButton")
        back.clicked.connect(self.leave_chat)
        top.addWidget(back)

        self.room_badge = QLabel("CHANNEL // ----")
        self.room_badge.setObjectName("roomBadge")
        top.addWidget(self.room_badge)
        top.addStretch()

        self.channel_indicator = QLabel("● SECURE LINK")
        self.channel_indicator.setObjectName("channelIndicator")
        top.addWidget(self.channel_indicator)
        layout.addLayout(top)

        self.chat_status = QLabel("WAITING FOR LINK…")
        self.chat_status.setObjectName("chatStatus")
        layout.addWidget(self.chat_status)

        self.log = QTextBrowser()
        self.log.setObjectName("messageView")
        self.log.setOpenExternalLinks(False)
        layout.addWidget(self.log, 1)

        row = QHBoxLayout()
        row.setSpacing(10)
        self.message_input = QLineEdit()
        self.message_input.setObjectName("messageInput")
        self.message_input.setPlaceholderText("TRANSMIT MESSAGE…")
        self.message_input.returnPressed.connect(self.send_message)
        send_btn = QPushButton("SEND  ↵")
        send_btn.setObjectName("primaryButton")
        send_btn.clicked.connect(self.send_message)
        row.addWidget(self.message_input, 1)
        row.addWidget(send_btn)
        layout.addLayout(row)

        expire = QLabel("MESSAGES PURGE AUTOMATICALLY  •  30 SECONDS AFTER DELIVERY")
        expire.setObjectName("purgeText")
        expire.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(expire)

    def initialize_ui(self):
        main = QVBoxLayout(self)
        main.setContentsMargins(0, 0, 0, 0)
        main.addWidget(self.home)
        main.addWidget(self.chat)
        self.chat.hide()

        # Small status clock and subtle title animation.
        self.ui_timer = QTimer(self)
        self.ui_timer.timeout.connect(self.update_clock)
        self.ui_timer.start(1000)

        self.pulse_timer = QTimer(self)
        self.pulse_timer.timeout.connect(self.pulse_status)
        self.pulse_timer.start(800)
        self.pulse_state = False

    def create_chat(self):
        self.nickname = self.name_input.text().strip() or "Anonymous"
        code = normalize_code(self.create_code.text())
        if not code:
            QMessageBox.warning(self, "Missing code", "Enter a room code such as Testing.")
            self.create_code.setFocus()
            return
        self.start_room(code, owner=True)

    def join_chat(self):
        self.nickname = self.name_input.text().strip() or "Anonymous"
        code = normalize_code(self.join_code.text())
        if not code:
            QMessageBox.warning(self, "Missing code", "Enter the room code.")
            self.join_code.setFocus()
            return
        self.start_room(code, owner=False)

    def start_room(self, code, owner):
        self.code = code
        self.topic = topic_for(code)
        self.cipher = key_for(code)
        self.room_active = True
        self.visible_messages.clear()
        self.log.clear()

        self.room_badge.setText(f"CHANNEL // {code}")
        self.chat_status.setText("ESTABLISHING SECURE LINK…")
        self.home.hide()
        self.chat.show()

        self.relay.subscribe(self.topic)
        self.add_system(
            "Room created." if owner else "Looking for people in this room…"
        )

        # Because MQTT is a relay, everyone who knows the code can simply subscribe.
        # The presence packet lets clients see that another app instance is alive.
        if owner:
            self.advertise_timer.start(5000)

        QTimer.singleShot(1200, self.publish_presence)

    def publish_presence(self):
        if not self.room_active or not self.cipher:
            return
        payload = {
            "v": 1,
            "kind": "presence",
            "sender": self.nickname,
            "client_id": new_client_id(),
            "time": int(time.time()),
        }
        encrypted = self.cipher.encrypt(json.dumps(payload).encode())
        self.relay.publish(self.topic, encrypted)

    def send_message(self):
        text = self.message_input.text().strip()
        if not text or not self.room_active or not self.cipher:
            return

        payload = {
            "v": 1,
            "kind": "message",
            "id": uuid.uuid4().hex,
            "sender": self.nickname,
            "text": text[:4000],
            "sent": int(time.time()),
        }
        encrypted = self.cipher.encrypt(json.dumps(payload).encode())
        if not self.relay.connected.is_set():
            self.chat_status.setText("RELAY NOT READY — MESSAGE QUEUED FOR RETRY")
        self.relay.publish(self.topic, encrypted)

        # Render locally immediately. This is the sender's receipt.
        self.receive_message(self.nickname, text)
        self.sound.sent()
        self.message_input.clear()

    def poll_events(self):
        while True:
            try:
                event, value = self.events.get_nowait()
            except queue.Empty:
                return

            if event == "status":
                self.relay_status.setText("RELAY: " + value.upper())
                if self.room_active:
                    self.chat_status.setText(value)
            elif event == "message":
                self.process_network_payload(value)
            elif event == "detail":
                self.relay_status.setText("Relay detail: " + str(value))

    def process_network_payload(self, payload_bytes):
        if not self.room_active or not self.cipher:
            return
        try:
            obj = json.loads(self.cipher.decrypt(payload_bytes).decode())
        except (InvalidToken, ValueError, json.JSONDecodeError):
            # Different room / wrong code / unrelated topic packet.
            return

        if obj.get("kind") == "presence":
            sender = str(obj.get("sender", "Someone"))[:32]
            if sender != self.nickname:
                self.chat_status.setText(f"{sender} is here")
            return

        if obj.get("kind") != "message":
            return

        sender = str(obj.get("sender", "Anonymous"))[:32]
        text = str(obj.get("text", ""))[:4000]

        # MQTT sends our own publish back to us as well. Avoid duplicating it;
        # local sender copy is already displayed.
        if sender == self.nickname:
            return

        # Timer starts when THIS CLIENT receives/displays the message.
        self.receive_message(sender, text)
        self.sound.received()

    def receive_message(self, sender, text):
        self.visible_messages.append(
            {
                "expires": time.monotonic() + READ_DELETE_SECONDS,
                "sender": sender,
                "text": text,
            }
        )
        self.render_messages()

    def expire_messages(self):
        now = time.monotonic()
        new = [m for m in self.visible_messages if m["expires"] > now]
        if len(new) != len(self.visible_messages):
            self.visible_messages = new
            self.render_messages()

    def render_messages(self):
        parts = [
            "<style>"
            "body{font-family:Consolas,monospace;background:#080a0d;color:#d9e0e7;}"
            ".wrap{margin:8px 2px;padding:11px 13px;border:1px solid #1d2630;"
            "background:#0c1116;border-radius:7px;}"
            ".me{border-color:#355f48;background:#0d1612;}"
            ".name{font-size:11px;color:#78dca0;font-weight:700;letter-spacing:1px;}"
            ".other .name{color:#9aa9ff;}"
            ".msg{font-size:14px;color:#e7edf2;margin-top:4px;}"
            ".system{color:#657381;font-size:11px;margin:12px 4px;}"
            "</style>"
        ]
        for msg in self.visible_messages:
            own = msg["sender"] == self.nickname
            cls = "wrap me" if own else "wrap other"
            parts.append(
                f'<div class="{cls}">'
                f'<div class="name">{msg["sender"]}{" // YOU" if own else ""}</div>'
                f'<div class="msg">{self._escape_html(msg["text"])}</div>'
                f'</div>'
            )
        self.log.setHtml("".join(parts))

    @staticmethod
    def _escape_html(value):
        return (value.replace("&", "&amp;")
                    .replace("<", "&lt;")
                    .replace(">", "&gt;")
                    .replace('"', "&quot;")
                    .replace("'", "&#39;"))

    def add_system(self, text):
        current = self.log.toHtml()
        safe = self._escape_html(text)
        self.log.setHtml(current + f'<div class="system">[ {safe} ]</div>')

    def leave_chat(self):
        self.room_active = False
        self.advertise_timer.stop()
        self.visible_messages.clear()
        self.log.clear()
        self.chat.hide()
        self.home.show()
        self.message_input.clear()

    def update_clock(self):
        from datetime import datetime
        self.clock_label.setText(datetime.now().strftime("%H:%M:%S"))

    def pulse_status(self):
        if not hasattr(self, "relay_dot"):
            return
        self.pulse_state = not getattr(self, "pulse_state", False)
        if "CONNECTED" in self.relay_status.text() or "Connected" in self.relay_status.text():
            self.relay_dot.setStyleSheet(
                "color: #66ff99;" if self.pulse_state else "color: #2f8f55;"
            )
        else:
            self.relay_dot.setStyleSheet(
                "color: #ff3355;" if self.pulse_state else "color: #733044;"
            )

    def closeEvent(self, event):
        self.room_active = False
        self.advertise_timer.stop()
        self.relay.stop()
        event.accept()


APP_STYLE = r"""
QWidget {
    background: #080a0d;
    color: #d9e0e7;
    font-family: "Consolas", "Cascadia Mono", monospace;
    font-size: 13px;
}
QLabel { background: transparent; }
#logoMark { color: #69f5a0; }
#brandTitle { color: #eafaf0; letter-spacing: 4px; }
#brandSub { color: #668074; letter-spacing: 2px; font-size: 10px; }
#statusDot { color: #69f5a0; font-size: 15px; }
#statusText { color: #7f8e99; letter-spacing: 1px; font-size: 10px; }
#clock { color: #56636d; letter-spacing: 1px; }
#accentLine { background: #1a6d43; }
#panel {
    background: #0d1116;
    border: 1px solid #1a232c;
    border-radius: 8px;
}
#panel:hover { border: 1px solid #27483a; }
#sectionLabel { color: #6f8b7c; font-size: 10px; letter-spacing: 2px; }
#dimText { color: #4e5a63; font-size: 10px; }
QLineEdit {
    background: #080b0f;
    border: 1px solid #25303a;
    border-radius: 6px;
    padding: 11px 12px;
    color: #e9f1f5;
    selection-background-color: #173a29;
}
QLineEdit:focus { border: 1px solid #4d9d72; }
QLineEdit::placeholder { color: #43515a; }
QPushButton {
    border-radius: 6px;
    padding: 11px 16px;
    font-weight: 700;
    letter-spacing: 1px;
}
#primaryButton {
    background: #163624;
    color: #8df3b2;
    border: 1px solid #2e7350;
}
#primaryButton:hover { background: #1b452d; }
#primaryButton:pressed { background: #10291b; }
#secondaryButton {
    background: #10161c;
    color: #a4b0b9;
    border: 1px solid #2b3741;
}
#secondaryButton:hover { background: #151e25; color: #d6e0e6; }
#ghostButton {
    background: transparent;
    color: #7b8b96;
    border: 1px solid #1e2730;
    padding: 8px 12px;
}
#ghostButton:hover { color: #d3dde4; border-color: #3b4a56; }
#console {
    background: #06080a;
    border: 1px solid #141c22;
    border-radius: 6px;
}
#consoleText {
    color: #4c8b67;
    font-size: 10px;
    line-height: 1.4;
}
#footer { color: #39434b; font-size: 9px; letter-spacing: 1px; }
#roomBadge {
    background: #0f1712;
    border: 1px solid #28533b;
    color: #7eeea5;
    border-radius: 5px;
    padding: 8px 11px;
    letter-spacing: 1px;
}
#channelIndicator { color: #71dc96; letter-spacing: 1px; font-size: 10px; }
#chatStatus { color: #62727e; font-size: 10px; letter-spacing: 2px; }
#messageView {
    background: #06090c;
    border: 1px solid #182029;
    border-radius: 8px;
    padding: 7px;
}
#messageInput {
    background: #0b1015;
    border: 1px solid #28343e;
    padding: 12px;
}
#purgeText { color: #40504a; font-size: 9px; letter-spacing: 1px; }
QScrollBar:vertical {
    background: #080a0d;
    width: 8px;
    margin: 2px;
}
QScrollBar::handle:vertical {
    background: #26332d;
    min-height: 30px;
    border-radius: 4px;
}
QScrollBar::add-line, QScrollBar::sub-line { height: 0px; }
"""

if __name__ == "__main__":
    app = QApplication([])
    app.setApplicationName(APP_NAME)
    app.setApplicationDisplayName(APP_NAME)
    app.setWindowIcon(QIcon(resource_path("KrakenSpy.ico")))
    app.setStyleSheet(APP_STYLE)
    window = BlinkChat()
    window.initialize_ui()
    window.show()
    app.exec()
