@echo off
setlocal enabledelayedexpansion

title AtmoSync GitHub Sync & Upload Tool
echo =====================================================================
echo           AtmoSync Telemetry: GitHub Auto-Sync & Push
echo           Target Branch: feature/Darshan
echo =====================================================================
echo.

set "ROOT_DIR=D:\Infotact Internship"
set "REPO_DIR=D:\Infotact Internship\AtmoSync_Micro_Climate_Arbitrage_Analytics"
set "DBT_DIR=D:\Infotact Internship\dbt_atmosync"

echo [1/5] Synchronizing latest changes into Git repository...

:: Sync models
if exist "%DBT_DIR%\models\staging\stg_sensor_events_v1.sql" (
    copy /Y "%DBT_DIR%\models\staging\stg_sensor_events_v1.sql" "%REPO_DIR%\models\staging\" >nul
)
if exist "%DBT_DIR%\models\staging\_stg_sensor_events_v1.yml" (
    copy /Y "%DBT_DIR%\models\staging\_stg_sensor_events_v1.yml" "%REPO_DIR%\models\staging\" >nul
)

:: Sync scripts
if exist "%DBT_DIR%\scripts\kafka_to_snowflake_consumer.py" (
    copy /Y "%DBT_DIR%\scripts\kafka_to_snowflake_consumer.py" "%REPO_DIR%\scripts\" >nul
)

:: Sync docs
if exist "%DBT_DIR%\docs\KAFKA_SNOWFLAKE_INGESTION.md" (
    copy /Y "%DBT_DIR%\docs\KAFKA_SNOWFLAKE_INGESTION.md" "%REPO_DIR%\docs\" >nul
)

:: Sync requirements
if exist "%DBT_DIR%\requirements-ingestion.txt" (
    copy /Y "%DBT_DIR%\requirements-ingestion.txt" "%REPO_DIR%\" >nul
)

:: Sync tests
if exist "%DBT_DIR%\tests" (
    xcopy /Y /Q "%DBT_DIR%\tests\assert_*.sql" "%REPO_DIR%\tests\" >nul 2>&1
    xcopy /Y /Q "%DBT_DIR%\tests\test_*.py" "%REPO_DIR%\tests\" >nul 2>&1
)

:: Sync root configs
if exist "%ROOT_DIR%\snowflake_setup.sql" (
    copy /Y "%ROOT_DIR%\snowflake_setup.sql" "%REPO_DIR%\" >nul
)
if exist "%ROOT_DIR%\docker-compose.superset.yml" (
    copy /Y "%ROOT_DIR%\docker-compose.superset.yml" "%REPO_DIR%\" >nul
)

echo [OK] Synchronization complete.
echo.

cd /d "%REPO_DIR%"

echo [2/5] Checking Git branch...
git checkout feature/Darshan
if errorlevel 1 (
    echo [ERROR] Failed to switch to branch feature/Darshan.
    pause
    exit /b 1
)

echo.
echo [3/5] Inspecting working directory changes...
git status --short
echo.

set "COMMIT_MSG="
set /p COMMIT_MSG="Enter commit message (Press Enter for default): "
if "!COMMIT_MSG!"=="" (
    set "COMMIT_MSG=Update Task 2 Warehouse & ELT code on feature/Darshan"
)

echo.
echo [4/5] Staging and committing changes...
git add -A
git commit -m "!COMMIT_MSG!"
if errorlevel 1 (
    echo [INFO] No new changes to commit or commit completed.
)

echo.
echo [5/5] Pushing to GitHub (origin feature/Darshan)...
git push origin feature/Darshan

if errorlevel 1 (
    echo.
    echo =====================================================================
    echo [ERROR] Push failed. Check your internet connection or GitHub credentials.
    echo =====================================================================
) else (
    echo.
    echo =====================================================================
    echo [SUCCESS] All code successfully uploaded to GitHub!
    echo Branch:  feature/Darshan
    echo Repo:    https://github.com/RohanVipin/AtmoSync_Micro_Climate_Arbitrage_Analytics.git
    echo =====================================================================
)

echo.
pause
