@echo off

REM Ir para a pasta do projeto
cd /d "C:\Users\felip\Documents\miner-ecom"

REM Rodar o batch Amazon-first com o Python CERTO
"C:\Users\felip\AppData\Local\Programs\Python\Python313\python.exe" crawler_amazon_batch.py --max-items 50 --max-tasks 20

REM Deixa a janela aberta se rodar manual
pause
