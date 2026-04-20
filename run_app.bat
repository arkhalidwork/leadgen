@echo off
setlocal

set ROOT_DIR=%~dp0
cd /d "%ROOT_DIR%"

set VENV_PY=%ROOT_DIR%venv\Scripts\python.exe

if not exist "%VENV_PY%" (
  echo Python virtual environment not found: %VENV_PY%
  echo Create it first, then install requirements.
  exit /b 1
)

echo Starting LeadGen...
"%VENV_PY%" app.py

endlocal
