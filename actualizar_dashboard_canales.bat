@echo off
title Actualizando Dashboard Canales...
cd /d "C:\Users\USUARIO\OneDrive - Avalogic S.A.S\Escritorio\analisis predictivo"
echo.
echo ================================================
echo   DASHBOARD CANALES VOZ / WHATSAPP - AVALOGIC
echo ================================================
echo.
echo Procesando archivos en WA-IVR...
echo.
C:\Users\USUARIO\AppData\Local\Python\pythoncore-3.14-64\python.exe actualizar_canales.py
echo.
if %errorlevel% == 0 (
    echo ================================================
    echo   Listo! Dashboard actualizado en:
    echo   https://gtr-ai421.github.io/avalogic-dashboards/canales/
    echo ================================================
) else (
    echo ERROR durante la actualizacion.
)
echo.
pause
