@echo off
cd /d "%~dp0"
call .venv\Scripts\activate.bat
python etf_tracker.py
pause
