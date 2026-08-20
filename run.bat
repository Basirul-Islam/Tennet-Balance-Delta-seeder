@echo off
setlocal
cd /d "%~dp0"

if not exist ".venv" (
    echo Creating virtual environment...
    py -3 -m venv .venv || goto :fail
)

call ".venv\Scripts\activate.bat" || goto :fail

python -c "import flask, pymysql, requests, zoneinfo; zoneinfo.ZoneInfo('Europe/Amsterdam')" 2>nul
if errorlevel 1 (
    echo Installing dependencies...
    python -m pip install --quiet --upgrade pip
    python -m pip install --quiet -r requirements.txt || goto :fail
)

echo.
echo   TenneT Balance Delta seeder
echo   http://127.0.0.1:5057
echo.
start "" http://127.0.0.1:5057
python app.py
goto :eof

:fail
echo.
echo Setup failed. See the error above.
pause
exit /b 1
