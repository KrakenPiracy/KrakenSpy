
import base64
import hashlib
import hmac
from pathlib import Path
import json
import os
import sys
import queue
import random
import ssl
import threading
import time
import uuid

import paho.mqtt.client as mqtt
from cryptography.fernet import Fernet, InvalidToken
from PySide6.QtCore import QBuffer, QByteArray, QEvent, QIODevice, QSettings, QSize, QTimer, Qt, QUrl
from PySide6.QtMultimedia import QAudioOutput, QMediaPlayer
from PySide6.QtGui import QDesktopServices, QFont, QIcon, QImageReader, QKeySequence, QPixmap, QShortcut
from PySide6.QtWidgets import (
    QApplication,
    QFrame,
    QDialog,
    QFileDialog,
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

# Use encrypted relay transports only. The message payload is also encrypted
# before it reaches the relay, but TLS protects the MQTT connection itself.
RELAY_ENDPOINTS = [
    ("MQTT TLS", 8883, True, None),
    ("MQTT Secure WebSocket", 8084, True, "/mqtt"),
]
READ_DELETE_SECONDS = 30
IMAGE_DELETE_SECONDS = 10 * 60
IMAGE_TRANSFER_TIMEOUT_SECONDS = 2 * 60
MAX_SOURCE_IMAGE_BYTES = 16 * 1024 * 1024
MAX_IMAGE_BYTES = 1 * 1024 * 1024
MAX_IMAGE_DIMENSION = 1280
# Base64 and Fernet add overhead; this keeps each MQTT publish well below 128 KB.
IMAGE_CHUNK_BYTES = 24 * 1024
IMAGE_MIME_TYPES = {
    ".png": "image/png", ".jpg": "image/jpeg", ".jpeg": "image/jpeg",
    ".gif": "image/gif", ".webp": "image/webp", ".bmp": "image/bmp",
}

# Shared only through the public code-derived topic. Message contents are separately
# encrypted with the same normalized room code.
TOPIC_PREFIX = "blinkchat/v5/rooms/"
PUBLIC_ROOM_CODE = "KRAKENSPY-PUBLIC"
VERIFICATION_CODE_DIGEST = "0049de4c727d50e96be0575cbc593290a659278e5781e9df50fc006b61a60943"



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


def set_windows_app_id():
    """Let Windows assign this process its own taskbar icon instead of Python's."""
    if sys.platform != "win32":
        return
    try:
        import ctypes
        ctypes.windll.shell32.SetCurrentProcessExplicitAppUserModelID("KrakenSpy.KrakenSpy.1")
    except Exception:
        pass


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
        self.subscriptions = set()

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
            # MQTT subscriptions belong to the connection. Re-subscribe after
            # every reconnect so a brief network drop does not silently stop
            # incoming messages.
            with self.lock:
                topics = tuple(self.subscriptions)
            for topic in topics:
                try:
                    rc, _mid = client.subscribe(topic, qos=1)
                    if rc != mqtt.MQTT_ERR_SUCCESS:
                        self.events.put(("status", f"Re-subscribe failed ({rc})"))
                except Exception as exc:
                    self.events.put(("status", f"Re-subscribe error: {exc}"))
        else:
            self.connected.clear()
            self.events.put(("status", f"Broker refused connection ({reason_code})"))

    def _on_disconnect(self, client, userdata, disconnect_flags, reason_code, properties):
        self.connected.clear()

    def _on_message(self, client, userdata, msg):
        # This is the only thing the MQTT callback does with application data.
        self.events.put(("message", bytes(msg.payload)))

    def subscribe(self, topic):
        with self.lock:
            self.subscriptions.add(topic)

        def worker():
            if not self.connected.wait(15):
                self.events.put(("status", "Still trying to connect to relay…"))
                return
            try:
                with self.lock:
                    client = self.client
                    still_needed = topic in self.subscriptions
                if client and still_needed:
                    rc, _mid = client.subscribe(topic, qos=1)
                    if rc != mqtt.MQTT_ERR_SUCCESS:
                        self.events.put(("status", f"Subscribe failed ({rc})"))
            except Exception as exc:
                self.events.put(("status", f"Subscribe error: {exc}"))

        threading.Thread(target=worker, daemon=True).start()

    def unsubscribe(self, topic):
        with self.lock:
            self.subscriptions.discard(topic)
            client = self.client
        if not client or not self.connected.is_set():
            return
        try:
            client.unsubscribe(topic)
        except Exception:
            pass

    def publish_immediate(self, topic, payload: bytes, retain=False, timeout=2):
        """Publish small control packets immediately when the GUI needs a best-effort result."""
        if not self.connected.is_set():
            return False
        try:
            with self.lock:
                client = self.client
            if not client:
                return False
            info = client.publish(topic, payload=payload, qos=1, retain=retain)
            if info.rc != mqtt.MQTT_ERR_SUCCESS:
                return False
            info.wait_for_publish(timeout=timeout)
            return True
        except Exception:
            return False

    def publish(self, topic, payload: bytes, retain=False):
        def worker():
            if not self.connected.wait(15):
                self.events.put(("status", "Still connecting — message not sent"))
                return
            try:
                with self.lock:
                    client = self.client
                if client:
                    info = client.publish(topic, payload=payload, qos=1, retain=retain)
                    if info.rc != mqtt.MQTT_ERR_SUCCESS:
                        self.events.put(("status", f"Send failed ({info.rc})"))
            except Exception as exc:
                self.events.put(("status", f"Send error: {exc}"))

        threading.Thread(target=worker, daemon=True).start()

    def publish_batch(self, topic, payloads):
        """Publish a transfer sequentially without creating a thread per chunk."""
        def worker():
            if not self.connected.wait(15):
                self.events.put(("status", "Still connecting — image not sent"))
                return
            try:
                with self.lock:
                    client = self.client
                if not client:
                    return
                for payload in payloads:
                    if self.stop_event.is_set() or not self.connected.is_set():
                        self.events.put(("status", "Image transfer interrupted"))
                        return
                    info = client.publish(topic, payload=payload, qos=1, retain=False)
                    if info.rc != mqtt.MQTT_ERR_SUCCESS:
                        self.events.put(("status", f"Image send failed ({info.rc})"))
                        return
                    info.wait_for_publish(timeout=15)
            except Exception as exc:
                self.events.put(("status", f"Image send error: {exc}"))

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


class ImagePreviewDialog(QDialog):
    """A temporary, full-screen image viewer. Clicking it closes the viewer."""

    def __init__(self, image_bytes, parent=None):
        super().__init__(parent)
        self.setWindowTitle("KrakenSpy Image")
        self.setStyleSheet("background:#050709;")
        self._pixmap = QPixmap()
        self._pixmap.loadFromData(image_bytes)

        layout = QVBoxLayout(self)
        layout.setContentsMargins(24, 24, 24, 24)
        self.image_label = QLabel()
        self.image_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        self.image_label.installEventFilter(self)
        layout.addWidget(self.image_label, 1)
        hint = QLabel("CLICK ANYWHERE OR PRESS ESC TO CLOSE")
        hint.setObjectName("purgeText")
        hint.setAlignment(Qt.AlignmentFlag.AlignCenter)
        hint.installEventFilter(self)
        layout.addWidget(hint)

    def showEvent(self, event):
        super().showEvent(event)
        self._fit_image()

    def resizeEvent(self, event):
        super().resizeEvent(event)
        self._fit_image()

    def mousePressEvent(self, event):
        self.accept()

    def eventFilter(self, watched, event):
        if event.type() == QEvent.Type.MouseButtonPress:
            self.accept()
            return True
        return super().eventFilter(watched, event)

    def _fit_image(self):
        if not self._pixmap.isNull():
            self.image_label.setPixmap(self._pixmap.scaled(
                self.image_label.size(),
                Qt.AspectRatioMode.KeepAspectRatio,
                Qt.TransformationMode.SmoothTransformation,
            ))


class BlinkChat(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle(APP_NAME)
        self.setWindowIcon(QIcon(resource_path("KrakenSpy.ico")))
        self.window_settings = QSettings("KrakenSpy", "KrakenSpy")
        saved_geometry = self.window_settings.value("window_geometry")
        if saved_geometry:
            self.restoreGeometry(saved_geometry)
        else:
            # Compact first-launch size; later launches use the user's last size.
            self.resize(565, 670)
        self.setMinimumSize(520, 620)
        self.sound = SoundPlayer()
        self.sound.sent_sound = str(Path(__file__).with_name("Sent.mp3"))
        self.sound.receive_sound = str(Path(__file__).with_name("Receive.mp3"))

        self.nickname = ""
        self.code = ""
        self.topic = ""
        self.cipher = None
        self.room_active = False
        self.is_public_room = False
        self.is_host = False
        self.host_name = ""
        self.client_id = new_client_id()
        self.verified = False
        self.verified_badge_data = self._load_verified_badge()
        self.visible_messages = []
        self.image_transfers = {}
        self.events = queue.Queue()
        self.relay = Relay(self.events)
        self.relay.start()

        self.build_home()
        self.build_chat()
        self.build_verification_dialog()
        self.verification_shortcut = QShortcut(QKeySequence("F8"), self)
        self.verification_shortcut.activated.connect(self.open_verification_dialog)

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

        public_frame = QFrame()
        public_frame.setObjectName("publicPanel")
        pf = QHBoxLayout(public_frame)
        pf.setContentsMargins(18, 14, 18, 14)
        public_copy = QVBoxLayout()
        public_copy.addWidget(QLabel("PUBLIC ROOM // OPEN TO EVERYONE", objectName="sectionLabel"))
        public_hint = QLabel("No code required. The first client to initialize it is shown as host. Do not share private information here.")
        public_hint.setObjectName("dimText")
        public_hint.setWordWrap(True)
        public_copy.addWidget(public_hint)
        pf.addLayout(public_copy, 1)
        public_btn = QPushButton("JOIN PUBLIC ROOM")
        public_btn.setObjectName("publicButton")
        public_btn.clicked.connect(self.join_public_chat)
        pf.addWidget(public_btn)
        layout.addWidget(public_frame)

        console = QFrame()
        console.setObjectName("console")
        cl = QVBoxLayout(console)
        cl.setContentsMargins(14, 10, 14, 10)
        self.boot_console = QLabel("> krakenspy core loaded\n> secure ephemeral mode: READY\n> waiting for channel command…")
        self.boot_console.setObjectName("consoleText")
        self.boot_console.setWordWrap(True)
        cl.addWidget(self.boot_console)
        layout.addWidget(console)

        community = QHBoxLayout()
        community.setSpacing(10)
        website_btn = QPushButton("WEBSITE")
        website_btn.setObjectName("communityButton")
        website_btn.clicked.connect(lambda: self.open_external_url("https://krakenpiracy.netlify.app/"))
        discord_btn = QPushButton("DISCORD")
        discord_btn.setObjectName("communityButton")
        discord_btn.clicked.connect(lambda: self.open_external_url("https://dsc.gg/krakenpiracy"))
        community.addStretch()
        community.addWidget(website_btn)
        community.addWidget(discord_btn)
        community.addStretch()
        layout.addLayout(community)

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
        # Image URLs are click actions, not documents for QTextBrowser to load.
        self.log.setOpenLinks(False)
        self.log.setOpenExternalLinks(False)
        self.log.anchorClicked.connect(self.open_image_preview)
        self.log.viewport().setMouseTracking(True)
        self.log.viewport().installEventFilter(self)
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
        image_btn = QPushButton("IMAGE")
        image_btn.setObjectName("secondaryButton")
        image_btn.clicked.connect(self.choose_and_send_image)
        row.addWidget(self.message_input, 1)
        row.addWidget(image_btn)
        row.addWidget(send_btn)
        layout.addLayout(row)

        expire = QLabel("MESSAGES PURGE AUTOMATICALLY  •  30 SECONDS AFTER DELIVERY")
        expire.setObjectName("purgeText")
        expire.setText("MESSAGES PURGE AFTER 30 SECONDS  |  IMAGES PURGE AFTER 10 MINUTES")
        expire.setAlignment(Qt.AlignmentFlag.AlignCenter)
        layout.addWidget(expire)

    @staticmethod
    def open_external_url(url):
        QDesktopServices.openUrl(QUrl(url))

    def build_verification_dialog(self):
        self.verification_dialog = QDialog(self)
        self.verification_dialog.setWindowTitle("Community Badge")
        self.verification_dialog.setModal(True)
        self.verification_dialog.setFixedWidth(330)
        layout = QVBoxLayout(self.verification_dialog)
        layout.setContentsMargins(22, 20, 22, 20)
        layout.setSpacing(12)
        title = QLabel("COMMUNITY BADGE")
        title.setObjectName("sectionLabel")
        layout.addWidget(title)
        self.verification_input = QLineEdit()
        self.verification_input.setPlaceholderText("ACCESS CODE")
        self.verification_input.setEchoMode(QLineEdit.EchoMode.Password)
        self.verification_input.returnPressed.connect(self.submit_verification)
        layout.addWidget(self.verification_input)
        self.verification_status = QLabel("Local community badge only — not identity verification.")
        self.verification_status.setObjectName("dimText")
        layout.addWidget(self.verification_status)
        verify_button = QPushButton("VERIFY")
        verify_button.setObjectName("primaryButton")
        verify_button.clicked.connect(self.submit_verification)
        layout.addWidget(verify_button)

    def open_verification_dialog(self):
        self.verification_input.clear()
        self.verification_status.setText("Local community badge only — not identity verification.")
        self.verification_dialog.show()
        self.verification_dialog.raise_()
        self.verification_dialog.activateWindow()
        self.verification_input.setFocus()

    def submit_verification(self):
        attempt = self.verification_input.text().encode()
        attempt_digest = hashlib.sha256(attempt).hexdigest()
        if hmac.compare_digest(attempt_digest, VERIFICATION_CODE_DIGEST):
            self.verified = True
            self.verification_dialog.accept()
            self.render_messages()
        else:
            self.verification_status.setText("Badge code rejected.")
            self.verification_input.selectAll()

    @staticmethod
    def _load_verified_badge():
        try:
            with open(resource_path("verified.png"), "rb") as image_file:
                encoded = base64.b64encode(image_file.read()).decode("ascii")
            # QTextBrowser does not reliably apply CSS width/height to data-URI
            # images, so use HTML attributes to keep this badge icon-sized.
            return f'<img class="verified" width="14" height="14" src="data:image/png;base64,{encoded}">'
        except OSError:
            return ""

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
            QMessageBox.warning(self, "Missing code", "Enter a room code.")
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

    def join_public_chat(self):
        self.nickname = self.name_input.text().strip() or "Anonymous"
        self.start_room(PUBLIC_ROOM_CODE, owner=False, public=True)

    def start_room(self, code, owner, public=False):
        self.code = code
        self.topic = topic_for(code)
        self.cipher = key_for(code)
        self.room_active = True
        self.is_public_room = public
        self.is_host = owner
        self.host_name = self.nickname if owner else ""
        self.visible_messages.clear()
        self.image_transfers.clear()
        self.log.clear()

        self.room_badge.setText("CHANNEL // PUBLIC" if public else f"CHANNEL // {code}")
        self.chat_status.setText("ESTABLISHING SECURE LINK…")
        self.home.hide()
        self.chat.show()

        self.relay.subscribe(self.topic)
        self.add_system(
            "Room created." if owner else "Looking for people in this room…"
        )

        if public:
            self.add_system("Public room joined. This channel is visible to everyone using KrakenSpy.")
            # Give MQTT a moment to deliver an existing retained host record.
            # A small random delay reduces simultaneous host claims when several
            # clients enter an empty public room at the same time.
            QTimer.singleShot(random.randint(1200, 2600), self.claim_public_host_if_needed)

        # Because MQTT is a relay, everyone who knows the code can simply subscribe.
        # The presence packet lets clients see that another app instance is alive.
        if owner:
            self.advertise_timer.start(5000)

        QTimer.singleShot(1200, self.publish_presence)

    def claim_public_host_if_needed(self):
        if not self.room_active or not self.is_public_room or self.host_name:
            return
        self.is_host = True
        self.host_name = self.nickname
        payload = {
            "v": 1,
            "kind": "host",
            "sender": self.nickname,
            "client_id": self.client_id,
            "time": int(time.time()),
        }
        encrypted = self.cipher.encrypt(json.dumps(payload).encode())
        self.relay.publish(self.topic, encrypted, retain=True)
        self.advertise_timer.start(5000)
        self.chat_status.setText("PUBLIC ROOM HOST // KEEPING PRESENCE ACTIVE")
        self.add_system("You initialized the public room and are its host.")

    def publish_presence(self):
        if not self.room_active or not self.cipher:
            return
        payload = {
            "v": 1,
            "kind": "presence",
            "sender": self.nickname,
            "client_id": self.client_id,
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
            "client_id": self.client_id,
            "text": text[:4000],
            "verified": self.verified,
            "sent": int(time.time()),
        }
        encrypted = self.cipher.encrypt(json.dumps(payload).encode())
        if not self.relay.connected.is_set():
            self.chat_status.setText("RELAY NOT READY — MESSAGE NOT SENT")
        self.relay.publish(self.topic, encrypted)

        # Render locally immediately. This is the sender's receipt.
        self.receive_message(self.nickname, text, self.verified)
        self.sound.sent()
        self.message_input.clear()

    def choose_and_send_image(self):
        if not self.room_active or not self.cipher:
            return
        filename, _selected_filter = QFileDialog.getOpenFileName(
            self,
            "Send image",
            "",
            "Images (*.png *.jpg *.jpeg *.gif *.webp *.bmp)",
        )
        if not filename:
            return
        try:
            image_path = Path(filename)
            if image_path.stat().st_size > MAX_SOURCE_IMAGE_BYTES:
                QMessageBox.warning(self, "Image too large", "Choose an image smaller than 16 MB.")
                return
        except OSError as exc:
            QMessageBox.warning(self, "Image unavailable", f"Could not read that image: {exc}")
            return

        if image_path.suffix.lower() not in IMAGE_MIME_TYPES:
            QMessageBox.warning(self, "Unsupported image", "Choose a PNG, JPEG, GIF, WebP, or BMP image.")
            return
        prepared = self.prepare_image_for_transfer(str(image_path))
        if prepared is None:
            QMessageBox.warning(self, "Image unavailable", "That image could not be decoded.")
            return
        image_bytes, mime_type = prepared
        if len(image_bytes) > MAX_IMAGE_BYTES:
            QMessageBox.warning(self, "Image too large", "This image could not be compressed below 1 MB.")
            return

        image_id = uuid.uuid4().hex
        chunks = [image_bytes[offset:offset + IMAGE_CHUNK_BYTES]
                  for offset in range(0, len(image_bytes), IMAGE_CHUNK_BYTES)]
        payloads = []
        for index, chunk in enumerate(chunks):
            packet = {
                "v": 1,
                "kind": "image_chunk",
                "id": image_id,
                "sender": self.nickname,
                "client_id": self.client_id,
                "verified": self.verified,
                "mime": mime_type,
                "index": index,
                "total": len(chunks),
                "data": base64.b64encode(chunk).decode("ascii"),
            }
            payloads.append(self.cipher.encrypt(json.dumps(packet, separators=(",", ":")).encode()))

        # Show the sender's temporary copy immediately, while the receiver
        # reconstructs the encrypted chunks in memory.
        self.receive_image(self.nickname, mime_type, image_bytes, self.verified, image_id)
        self.sound.sent()
        self.chat_status.setText(f"SENDING IMAGE // {len(image_bytes) // 1024} KB")
        self.relay.publish_batch(self.topic, payloads)

    @staticmethod
    def prepare_image_for_transfer(filename):
        """Decode once, downscale, and recompress before putting image data in chat HTML."""
        reader = QImageReader(filename)
        reader.setAutoTransform(True)
        source_size = reader.size()
        if source_size.isValid():
            target_size = source_size.scaled(
                QSize(MAX_IMAGE_DIMENSION, MAX_IMAGE_DIMENSION),
                Qt.AspectRatioMode.KeepAspectRatio,
            )
            if target_size != source_size:
                reader.setScaledSize(target_size)
        image = reader.read()
        if image.isNull():
            return None
        data = QByteArray()
        buffer = QBuffer(data)
        if not buffer.open(QIODevice.OpenModeFlag.WriteOnly):
            return None
        # A compact JPEG keeps rendering and in-memory chat history responsive.
        if not image.save(buffer, "JPEG", 78):
            return None
        return bytes(data), "image/jpeg"

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

        if obj.get("kind") == "host" and self.is_public_room:
            host_id = str(obj.get("client_id", ""))
            host_name = str(obj.get("sender", "Host"))[:32]
            if host_id:
                # Never demote a local host because of our own retained record.
                # A newer remote host announcement is allowed to win the simple
                # best-effort public-room election.
                self.host_name = host_name
                self.is_host = host_id == self.client_id
                role = "YOU ARE HOST" if self.is_host else f"HOST // {host_name}"
                self.chat_status.setText(f"PUBLIC ROOM // {role}")
            return

        if obj.get("kind") == "image_chunk":
            self.process_image_chunk(obj)
            return

        if obj.get("kind") != "message":
            return

        sender = str(obj.get("sender", "Anonymous"))[:32]
        text = str(obj.get("text", ""))[:4000]
        verified = bool(obj.get("verified", False))

        # MQTT sends our own publish back to us as well. Avoid duplicating it;
        # local sender copy is already displayed.
        if str(obj.get("client_id", "")) == self.client_id:
            return

        # Timer starts when THIS CLIENT receives/displays the message.
        self.receive_message(sender, text, verified)
        self.sound.received()

    def process_image_chunk(self, obj):
        sender = str(obj.get("sender", "Anonymous"))[:32]
        if str(obj.get("client_id", "")) == self.client_id:
            # The local copy was displayed before publishing the transfer.
            return
        image_id = str(obj.get("id", ""))
        total = obj.get("total")
        index = obj.get("index")
        if (not image_id or not isinstance(total, int) or not isinstance(index, int)
                or total < 1 or total > 256 or index < 0 or index >= total):
            return
        try:
            chunk = base64.b64decode(str(obj.get("data", "")), validate=True)
        except (ValueError, TypeError):
            return
        if not chunk or len(chunk) > IMAGE_CHUNK_BYTES:
            return

        transfer = self.image_transfers.get(image_id)
        if transfer is None:
            mime_type = str(obj.get("mime", ""))
            if mime_type not in IMAGE_MIME_TYPES.values():
                return
            transfer = {
                "sender": sender,
                "verified": bool(obj.get("verified", False)),
                "mime": mime_type,
                "total": total,
                "chunks": {},
                "received_bytes": 0,
                "expires": time.monotonic() + IMAGE_TRANSFER_TIMEOUT_SECONDS,
            }
            self.image_transfers[image_id] = transfer
        if transfer["total"] != total or transfer["sender"] != sender:
            return
        if index not in transfer["chunks"]:
            transfer["chunks"][index] = chunk
            transfer["received_bytes"] += len(chunk)
        if transfer["received_bytes"] > MAX_IMAGE_BYTES:
            self.image_transfers.pop(image_id, None)
            return
        if len(transfer["chunks"]) != total:
            return

        image_bytes = b"".join(transfer["chunks"][part] for part in range(total))
        self.image_transfers.pop(image_id, None)
        self.receive_image(
            transfer["sender"], transfer["mime"], image_bytes, transfer["verified"], image_id,
        )
        self.chat_status.setText(f"IMAGE RECEIVED // {len(image_bytes) // 1024} KB")
        self.sound.received()

    def receive_message(self, sender, text, verified=False):
        self.visible_messages.append(
            {
                "expires": time.monotonic() + READ_DELETE_SECONDS,
                "sender": sender,
                "text": text,
                "verified": verified,
            }
        )
        self.render_messages()

    def receive_image(self, sender, mime_type, image_bytes, verified=False, image_id=None):
        image_data = base64.b64encode(image_bytes).decode("ascii")
        thumbnail_data = self.make_image_thumbnail(image_bytes)
        self.visible_messages.append(
            {
                "expires": time.monotonic() + IMAGE_DELETE_SECONDS,
                "sender": sender,
                "mime": mime_type,
                "image_data": image_data,
                "thumbnail_data": thumbnail_data,
                "image_id": image_id or uuid.uuid4().hex,
                "verified": verified,
            }
        )
        self.render_messages()

    @staticmethod
    def make_image_thumbnail(image_bytes):
        pixmap = QPixmap()
        if not pixmap.loadFromData(image_bytes):
            return base64.b64encode(image_bytes).decode("ascii")
        thumbnail = pixmap.scaled(
            QSize(360, 360),
            Qt.AspectRatioMode.KeepAspectRatio,
            Qt.TransformationMode.SmoothTransformation,
        )
        data = QByteArray()
        buffer = QBuffer(data)
        if buffer.open(QIODevice.OpenModeFlag.WriteOnly) and thumbnail.save(buffer, "JPEG", 72):
            return base64.b64encode(bytes(data)).decode("ascii")
        return base64.b64encode(image_bytes).decode("ascii")

    def open_image_preview(self, url):
        if url.scheme() != "image":
            return
        image_id = url.path()
        image = next(
            (message for message in self.visible_messages
             if message.get("image_id") == image_id),
            None,
        )
        if not image:
            return
        try:
            image_bytes = base64.b64decode(image["image_data"], validate=True)
        except (KeyError, ValueError, TypeError):
            return
        preview = ImagePreviewDialog(image_bytes, self)
        preview.showFullScreen()
        preview.exec()

    def eventFilter(self, watched, event):
        if watched is self.log.viewport() and event.type() == QEvent.Type.MouseMove:
            is_image_link = self.log.anchorAt(event.position().toPoint()).startswith("image:")
            if is_image_link:
                watched.setCursor(Qt.CursorShape.PointingHandCursor)
                watched.setToolTip("Click to view full screen")
            else:
                watched.unsetCursor()
                watched.setToolTip("")
        return super().eventFilter(watched, event)

    def expire_messages(self):
        now = time.monotonic()
        self.image_transfers = {
            image_id: transfer for image_id, transfer in self.image_transfers.items()
            if transfer["expires"] > now
        }
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
            ".verified{width:14px;height:14px;vertical-align:middle;margin-left:5px;}"
            ".other .name{color:#9aa9ff;}"
            ".msg{font-size:14px;color:#e7edf2;margin-top:4px;}"
            ".imageName{font-size:10px;color:#7d919d;margin:6px 0 4px 0;}"
            ".sharedImage{border:1px solid #26343c;}"
            ".imageHeader{font-size:10px;color:#75dba0;letter-spacing:1px;}"
            ".system{color:#657381;font-size:11px;margin:12px 4px;}"
            "</style>"
        ]
        for msg in self.visible_messages:
            own = msg["sender"] == self.nickname
            cls = "wrap me" if own else "wrap other"
            badge = self.verified_badge_data if msg.get("verified") else ""
            if "image_data" in msg:
                image_url = f'image:{msg["image_id"]}'
                content = (
                    '<table align="left" width="374" border="1" bordercolor="#2e7350" '
                    'cellspacing="0" cellpadding="0"><tr><td bgcolor="#0a1510" cellpadding="5">'
                    '<div class="imageHeader">IMAGE TRANSMISSION</div>'
                    '</td></tr><tr><td bgcolor="#05090c" cellpadding="6">'
                    f'<a href="{image_url}" title="Click to view full screen">'
                    f'<img class="sharedImage" width="360" src="data:image/jpeg;base64,{msg["thumbnail_data"]}"></a>'
                    '</td></tr></table><br clear="all">'
                )
            else:
                content = f'<div class="msg">{self._escape_html(msg["text"])}</div>'
            header = (
                f'<div class="name">{self._escape_html(msg["sender"])}'
                f'{badge}{" // YOU" if own else ""}</div>'
            )
            parts.append(f'<div class="{cls}">{header}{content}</div>')
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
        if self.is_public_room and self.is_host and self.topic:
            # Clear the retained host marker so the next public-room user can
            # claim the role instead of inheriting a stale host.
            self.relay.publish_immediate(self.topic, b"", retain=True)
        if self.topic:
            self.relay.unsubscribe(self.topic)
        self.room_active = False
        self.advertise_timer.stop()
        self.visible_messages.clear()
        self.image_transfers.clear()
        self.is_public_room = False
        self.is_host = False
        self.host_name = ""
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
        self.window_settings.setValue("window_geometry", self.saveGeometry())
        if self.is_public_room and self.is_host and self.topic:
            self.relay.publish_immediate(self.topic, b"", retain=True)
        if self.topic:
            self.relay.unsubscribe(self.topic)
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
#publicPanel {
    background: #0d1512;
    border: 1px solid #28533b;
    border-radius: 8px;
}
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
#communityButton {
    background: #0b1218;
    color: #77b3d4;
    border: 1px solid #284253;
    padding: 7px 13px;
    font-size: 10px;
}
#communityButton:hover { background: #10212c; color: #a8dbf5; border-color: #46748d; }
#publicButton {
    background: #183d29;
    color: #a4f7bf;
    border: 1px solid #3d8a5d;
}
#publicButton:hover { background: #215237; }
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
    set_windows_app_id()
    app = QApplication([])
    app.setApplicationName(APP_NAME)
    app.setApplicationDisplayName(APP_NAME)
    app.setWindowIcon(QIcon(resource_path("KrakenSpy.ico")))
    app.setStyleSheet(APP_STYLE)
    window = BlinkChat()
    window.initialize_ui()
    window.show()
    app.exec()
