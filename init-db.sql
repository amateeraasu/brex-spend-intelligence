-- Creates the application database alongside the Airflow metadata DB.
-- Runs automatically on first postgres container start.
CREATE DATABASE brex_spend;
GRANT ALL PRIVILEGES ON DATABASE brex_spend TO airflow;
