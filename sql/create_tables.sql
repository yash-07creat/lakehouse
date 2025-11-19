-- create_tables.sql
CREATE DATABASE IF NOT EXISTS lakehouse;

CREATE TABLE IF NOT EXISTS lakehouse.bronze_rides
USING DELTA
LOCATION '/dbfs/storage/bronze/rides_table';

CREATE TABLE IF NOT EXISTS lakehouse.silver_rides
USING DELTA
LOCATION '/dbfs/storage/silver/rides_clean';

CREATE TABLE IF NOT EXISTS lakehouse.gold_daily_revenue
USING DELTA
LOCATION '/dbfs/storage/gold/daily_revenue';
