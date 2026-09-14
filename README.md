<div align="center">

# 🐙 KrakenSpy

### `PRIVATE // EPHEMERAL // OPEN SOURCE`

A lightweight desktop chat app built with Python, designed for quick, temporary conversations without accounts, profiles, or stored chat history.

[![Python](https://img.shields.io/badge/Python-3.12+-3776AB?style=flat-square&logo=python&logoColor=white)](https://www.python.org/)
[![Qt](https://img.shields.io/badge/GUI-PySide6-41CD52?style=flat-square&logo=qt&logoColor=white)](https://doc.qt.io/qtforpython/)
[![License](https://img.shields.io/badge/License-MIT-00a86b?style=flat-square)](LICENSE)
[![Status](https://img.shields.io/badge/Status-Early%20Development-orange?style=flat-square)]()

<img src="assets/KrakenSpy.png" width="180" alt="KrakenSpy logo">

[![Discord](https://img.shields.io/badge/Discord-Join%20the%20Server-5865F2?style=flat-square&logo=discord&logoColor=white)](https://dsc.gg/krakenpiracy)

</div>

---

## ⚡ What is KrakenSpy?

KrakenSpy is a small Windows chat application built around one simple idea:

> **Enter a room code, talk, and leave almost nothing behind on your computer.**

There are no user accounts, no profile system, and no local chat database. The current version uses a public MQTT relay so two people can communicate even when they are on completely different networks.

Messages are encrypted by the application before they are published to the relay, and each client removes displayed messages after **30 seconds**.

The project is open source so you can inspect, modify, build, and experiment with it yourself.

---

## 🖥️ Current Features

| Feature | Status |
|---|:---:|
| Room-code based chat | ✅ |
| Different Wi-Fi / networks | ✅ |
| No account required | ✅ |
| No local message database | ✅ |
| Encrypted message payloads | ✅ |
| Messages disappear after 30 seconds | ✅ |
| Send notification sound | ✅ |
| Receive notification sound | ✅ |
| KrakenSpy logo / Windows icon | ✅ |
| Windows `.exe` build script | ✅ |
| Modern terminal-inspired UI | ✅ |
| Real end-to-end identity verification | 🚧 |
| Decentralized relay / true serverless networking | 🚧 |
| Mobile client | 🚧 |

---

## 🕶️ How it works

KrakenSpy currently uses a **relay-based architecture**.

```text
       ┌─────────────────┐
       │     Person A    │
       │    KrakenSpy    │
       └────────┬────────┘
                │
                │ encrypted payload
                ▼
       ┌─────────────────┐
       │   MQTT Relay    │
       │   public test   │
       └────────┬────────┘
                │
                │ encrypted payload
                ▼
       ┌─────────────────┐
       │     Person B    │
       │    KrakenSpy    │
       └─────────────────┘
```

Both clients make outbound connections to the relay, which means users do **not** need to be on the same Wi-Fi network and normally do not need router port forwarding.

The relay is used for transport only. KrakenSpy does not maintain a chat-history database.

### Encryption

The current prototype derives a symmetric encryption key from the room code and encrypts message payloads before sending them.

That means you should treat the room code like a password:

```text
Bad:
TEST
HELLO
1234

Better:
KRAKEN-7F4B-91D2-A8E6
```

A predictable room code is easier to guess. For genuinely sensitive communication, use a long random room code.

---

## ⏱️ 30-second messages

When a message reaches a client, the local application starts a **30-second expiry timer**.

After the timer expires, the message is removed from the visible chat window.

```text
MESSAGE RECEIVED
       │
       ▼
   30 second timer
       │
       ▼
  MESSAGE PURGED
```

### Important privacy limitation

The 30-second rule currently applies to the **client's displayed copy**.

It is not a cryptographic guarantee that every intermediary system has instantly erased every byte of the packet. The current app uses a public MQTT testing relay, so this project should be considered an **early privacy-focused prototype**, not a production secure-messaging platform.

---

## 🎧 Notification sounds

KrakenSpy includes two small notification sounds:

- `Sent.mp3` — plays when you send a message.
- `Receive.mp3` — plays when a message arrives from someone else.

---

## 🎨 Interface

The UI is intentionally inspired by:

- old terminal / security consoles
- dark operating-system interfaces
- subtle green status indicators
- minimal cyber aesthetic
- modern spacing and typography

It is deliberately **not** designed as a colorful "gamer" or kids' chat app.

---

## 🚀 Run from source

### Requirements

- Windows
- Python 3.12+ recommended

Install dependencies:

```powershell
python -m pip install -r requirements.txt
```

Start KrakenSpy:

```powershell
python .\krakenspy.py
```

---

## 🔨 Build the Windows EXE

The repository includes a build script.

Run:

```text
build_exe.bat
```

The resulting application will be:

```text
dist/
└── KrakenSpy.exe
```

The build includes:

- KrakenSpy icon
- Sent sound
- Receive sound
- Python application
- Qt runtime

You can send the resulting `KrakenSpy.exe` to another Windows user without asking them to install Python.

---

## 📂 Project structure

```text
KrakenSpy/
│
├── krakenspy.py          # Main application
├── requirements.txt      # Python dependencies
├── run_windows.bat       # Run helper
├── build_exe.bat         # PyInstaller build script
├── KrakenSpy.ico         # Application icon
├── Sent.mp3              # Sent sound
├── Receive.mp3           # Receive sound
├── README.md             # This file
├── LICENSE               # MIT license
└── assets/
    └── KrakenSpy.png     # README logo
```

---

## 🧪 Quick test

You can test it with two copies of the application.

### Window A

```text
Nickname: Alex
Room: KRAKEN123
→ Create Chat
```

### Window B

```text
Nickname: Bob
Room: KRAKEN123
→ Join Chat
```

They can be on completely different internet connections.

Then send a message and wait 30 seconds. The displayed message should disappear.

---

## 🌐 Why not completely serverless?

A room name such as:

```text
KRAKEN123
```

cannot, by itself, tell a computer where another computer is on the internet.

Without some form of discovery/relay infrastructure, the application would need another way to locate the peer and handle NAT/firewalls.

The current project chooses **reliable connectivity first**.

The long-term goal is to investigate:

```text
Room code
   ↓
Peer discovery
   ↓
NAT traversal
   ↓
Direct encrypted connection
```

That would move KrakenSpy closer to a genuinely decentralized architecture.

---

## 🔐 Privacy & security notice

KrakenSpy is open source and intended for experimentation and privacy-focused communication.

However, **this is not yet a professionally audited secure messenger**.

In particular:

- The public MQTT relay is shared infrastructure.
- Room codes should be treated as secrets.
- The current design does not provide a verified identity system.
- Message expiry on a client is not proof of deletion from every network component.
- The project has not undergone an independent security audit.

**Do not use the current prototype for high-risk communications.**

---

## 💬 Community

Have questions, found a bug, or want to follow development?

**Join the Kraken community on Discord:**  
https://dsc.gg/krakenpiracy

## 🛠️ Roadmap

### v0.x

- [x] Basic room chat
- [x] Cross-network relay
- [x] Encrypted payloads
- [x] Ephemeral 30-second messages
- [x] Sound notifications
- [x] KrakenSpy branding
- [x] Windows EXE build

### Future

- [ ] Strong random room generation
- [ ] Read / delivered state
- [ ] Better message animations
- [ ] User presence panel
- [ ] Typing indicator
- [ ] Attachments
- [ ] Better identity verification
- [ ] Safer dedicated relay infrastructure
- [ ] STUN / ICE peer-to-peer mode
- [ ] Optional decentralized discovery
- [ ] Linux support
- [ ] Mobile client

---

## 🤝 Contributing

Pull requests, bug reports, ideas, and improvements are welcome.

Good first contributions include:

- UI improvements
- networking experiments
- security reviews
- documentation
- packaging
- Linux support
- testing different network environments

Before opening a large pull request, please explain the change and why it improves the project.

---

## 📜 License

KrakenSpy is released under the MIT License.

See [`LICENSE`](LICENSE).

---

<div align="center">

**KRAKENSPY**

`PRIVATE // EPHEMERAL // OPEN SOURCE`

Made for people who want a simple chat without turning it into a social network.

</div>
