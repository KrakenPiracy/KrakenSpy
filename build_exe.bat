@echo off
title KrakenSpy Builder
python -m pip install -r requirements.txt
python -m pip install pyinstaller
python -m PyInstaller --noconfirm --clean --onefile --windowed --name KrakenSpy --icon "KrakenSpy.ico" --add-data "KrakenSpy.ico;." --add-data "Sent.mp3;." --add-data "Receive.mp3;." krakenspy.py
echo.
echo Built: dist\KrakenSpy.exe
pause
