@echo off

REM Ir para a pasta do projeto
cd /d "C:\Users\felip\Documents\miner-ecom"

REM Rodar o batch Amazon-first com o MESMO Python do VS Code (.venv)
"C:\Users\felip\Documents\miner-ecom\.venv\Scripts\python.exe" crawler_amazon_batch.py --max-items 50 --max-tasks 20

REM Deixa a janela aberta se rodar manual
pause
