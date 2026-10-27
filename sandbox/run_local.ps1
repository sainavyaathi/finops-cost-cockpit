# Runs the accelerator end to end locally on synthetic System Tables.
# Usage (from the repo root):  .\sandbox\run_local.ps1
$root = Split-Path -Parent $PSScriptRoot

$env:JAVA_HOME = (Get-ChildItem "$env:LOCALAPPDATA\Programs\jdk17" -Directory | Select-Object -First 1).FullName
$env:HADOOP_HOME = "$env:LOCALAPPDATA\Programs\hadoop"
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
$env:PYSPARK_PYTHON = "$root\.venv\Scripts\python.exe"
$env:PYSPARK_DRIVER_PYTHON = $env:PYSPARK_PYTHON
$env:SPARK_HOME = "$root\.venv\Lib\site-packages\pyspark"

& $env:PYSPARK_PYTHON "$root\sandbox\run_local.py"
