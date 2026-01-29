@echo off
TITLE Bijouterie Zaher - Sales & Data Agent
COLOR 0A
ECHO =======================================================
ECHO   Bijouterie Zaher - Automatic Sync Agent
ECHO   ---------------------------------------
ECHO   Syncs Sales, Expenses, and Payroll to Cloud.
ECHO   Keeps Cloud Server Awake.
ECHO =======================================================
ECHO.
ECHO Starting Agent...
python auto_sync.py
PAUSE
