USE ROLE ACCOUNTADMIN;

-- Creating a dev database to have a blank sandbox
CREATE DATABASE IF NOT EXISTS DEV_DB
    COMMENT = 'Development Database for training with Snowflake';

-- Create a sandbox schema
CREATE SCHEMA IF NOT EXISTS DEV_DB.SANDBOX
    COMMENT = 'Personal sandbox for experimenting with';

-- Switch to our new DB and Schema
USE DATABASE DEV_DB;
USE SCHEMA SANDBOX;