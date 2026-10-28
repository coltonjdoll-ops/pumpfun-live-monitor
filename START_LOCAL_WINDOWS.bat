@echo off
cd /d "%~dp0"
where py >nul 2>&1
if %errorlevel%==0 (set "PY=py -3") else (set "PY=python")
%PY% -m pip install -r requirements.txt
if errorlevel 1 goto failed
echo Open http://127.0.0.1:8000
%PY% -m uvicorn app:app --host 127.0.0.1 --port 8000
:failed
pause
