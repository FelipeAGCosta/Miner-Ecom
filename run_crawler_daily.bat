@echo off
setlocal

REM 1) Ir para a pasta do projeto
cd /d "C:\Users\felip\Documents\miner-ecom"

REM 2) Criar pasta de logs (se ainda não existir)
if not exist "logs" mkdir "logs"

REM 3) Montar a data em AAAA-MM-DD para usar no nome do log
set DATA=%date:~-4%-%date:~3,2%-%date:~0,2%

REM 4) Rodar o crawler usando o Python do venv
"C:\Users\felip\Documents\miner-ecom\.venv\Scripts\python.exe" ^
  crawler_amazon_batch.py --max-items 30 --max-tasks 15 ^
  >> "logs\crawler_%DATA%.log" 2>&1

endlocal
