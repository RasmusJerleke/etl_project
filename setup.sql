DROP DATABASE IF EXISTS weather_db;
DROP USER IF EXISTS etl_user;

CREATE DATABASE weather_db ;
CREATE USER etl_user WITH ENCRYPTED PASSWORD '123';
GRANT ALL PRIVILEGES ON DATABASE weather_db TO etl_user;

\c weather_db etl_user