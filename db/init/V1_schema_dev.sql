-- This script is meant for creating the mock db so we can procceed with the rest of the implementation..
-- The only factor to consider is having installed and configured postgresql on your machine
-- obviously all documentation available on : https://www.postgresql.org/

-- This script is meant for creating the mock db so we can procceed with the rest of the implementation..
-- The only factor to consider is having installed and configured postgresql on your machine
-- obviously all documentation available on : https://www.postgresql.org/

-- this is the part of the implementation that is recommended for secure database in production.

-- The main application database is created by the container
-- via POSTGRES_DB (see docker-compose). We assume that is `eventdb`
-- and operate inside that database.
CREATE ROLE app_owner NOLOGIN;

-- user role for running the application as a normal user.
DROP ROLE IF EXISTS app_user;
CREATE ROLE app_user LOGIN PASSWORD 'user123';
-- migrator role for making changes for production.
DROP ROLE IF EXISTS migrator;
CREATE ROLE migrator LOGIN PASSWORD 'migrator123';
-- monitor role for monitoring the system database.
DROP ROLE IF EXISTS monitor;
CREATE ROLE monitor LOGIN PASSWORD 'monitor123';

DROP ROLE IF EXISTS tester;
CREATE ROLE tester LOGIN PASSWORD 'tester123';
GRANT SELECT ON ALL TABLES IN SCHEMA public TO tester;

ALTER DEFAULT PRIVILEGES IN SCHEMA public
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO app_user;
GRANT CONNECT ON DATABASE eventdb TO app_user;
GRANT USAGE ON SCHEMA public TO app_user;

GRANT CONNECT ON DATABASE eventdb TO migrator;
GRANT app_owner TO migrator;
ALTER DEFAULT PRIVILEGES FOR ROLE migrator IN SCHEMA public
GRANT ALL ON TABLES TO migrator;

-- i use the pg_monitor from postgresql
GRANT pg_monitor TO monitor;

-- i create mock table for inserting mock events...
DROP TABLE IF EXISTS events;
CREATE TABLE events (
    id VARCHAR(64) PRIMARY KEY,               -- unique event ID
    severity VARCHAR(10) NOT NULL,            -- fatal, error, warning, info, debug
    stack TEXT,                               -- detailed error stack
    type VARCHAR(100),                         -- error type or classification
    timestamp BIGINT NOT NULL,                -- client timestamp (ms)
    received_at BIGINT NOT NULL,              -- server receive timestamp (ms)
    -- The affected application resource, such as a route, endpoint, file,
    -- service, or other component where the event occurred.
    resource TEXT,
    -- The URL or page that referred the request to the affected resource.
    -- This describes the event origin, not the resource that failed.
    referrer TEXT,

    -- App metadata
    app_name VARCHAR(255) NOT NULL,          
    app_version VARCHAR(50),
    app_stage VARCHAR(50),

    -- Optional tags stored as JSON
    tags JSON,

    -- Endpoint / client info
    endpoint_id VARCHAR(64) NOT NULL,
    endpoint_language VARCHAR(10),
    endpoint_platform VARCHAR(50),
    endpoint_os VARCHAR(50),
    endpoint_os_version VARCHAR(20),
    endpoint_runtime VARCHAR(50),
    endpoint_runtime_version VARCHAR(20),
    endpoint_country CHAR(2),
    endpoint_user_agent TEXT,
    endpoint_device_type VARCHAR(20)
);

-- create indexes for faster querying
CREATE INDEX idx_severity ON events(severity);
CREATE INDEX idx_app_name ON events(app_name);
CREATE INDEX idx_app_stage ON events(app_stage);
CREATE INDEX idx_endpoint_country ON events(endpoint_country);
CREATE INDEX idx_timestamp ON events(timestamp);
CREATE INDEX idx_received_at ON events(received_at);

-- i insert for example 10 registers of events
INSERT INTO events (
    id, severity, stack, type, timestamp, received_at, resource, referrer,
    app_name, app_version, app_stage, tags,
    endpoint_id, endpoint_language, endpoint_platform, endpoint_os, endpoint_os_version,
    endpoint_runtime, endpoint_runtime_version, endpoint_country, endpoint_user_agent, endpoint_device_type
) VALUES
-- Fatal error from production web app
('evt-001', 'fatal', 'ReferenceError: x is not defined at main.js:45', 'ReferenceError', 1708213200000, 1708213200500, '/checkout', 'https://example.com/cart',
 'Pizzeria Frontend', '3.2.0', 'production', '{"userType": "premium", "trial": "no"}',
 'uuid-1234-5678-9012', 'en-US', 'MacIntel', 'MacOS', '12.3.1',
 'Chrome', '109.0.5414.119', 'US', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 12_3_1)...', 'Desktop'),

-- Error from staging backend service
('evt-002', 'error', 'TypeError: Cannot read property "orderId" of undefined at orders.js:78', 'TypeError', 1708216800000, 1708216801000, '/api/orders', 'https://staging.example.com/dashboard',
 'Pizzeria Backend', '1.8.5', 'staging', '{"env": "staging", "module": "orders"}',
 'uuid-2233-4455-6677', 'en-GB', 'Linux x86_64', 'Ubuntu', '22.04',
 'NodeJs', '18.15.0', 'GB', 'node-fetch/3.2', 'Server'),

-- Warning from testing mobile app
('evt-003', 'warning', 'NetworkError: Failed to fetch at api.js:32', 'NetworkError', 1708220400000, 1708220400200, '/api/menu', 'https://test.example.com/home',
 'Pizzeria Mobile', '2.5.1', 'testing', '{"userType": "freemium"}',
 'uuid-3344-5566-7788', 'es-ES', 'iPhone', 'iOS', '16.4',
 'Safari', '16.5', 'ES', 'Mozilla/5.0 (iPhone; CPU iPhone OS 16_4 like Mac OS X)...', 'Mobile'),

-- Info event for monitoring
('evt-004', 'info', NULL, 'Heartbeat', 1708224000000, 1708224000100, '/health', NULL,
 'Pizzeria Backend', '1.8.5', 'production', '{"service": "heartbeat"}',
 'uuid-4455-6677-8899', 'en-US', 'Windows NT', 'Windows', '10',
 'Edge', '111.0', 'US', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64)...', 'Desktop'),

-- Debug event from development app
('evt-005', 'debug', 'Debug info: user session active at session.js:12', 'DebugInfo', 1708227600000, 1708227600300, '/login', 'http://localhost:3000',
 'Pizzeria Frontend', '3.3.0-beta', 'development', '{"featureFlag": "new-login"}',
 'uuid-5566-7788-9900', 'fr-FR', 'MacIntel', 'MacOS', '13.0',
 'Chrome', '110.0.5481.178', 'FR', 'Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0)...', 'Desktop');

CREATE TABLE IF NOT EXISTS alerts (
    id VARCHAR(64) PRIMARY KEY,
    severity VARCHAR(10) NOT NULL,
    resource TEXT,
    payload JSON,
    created_at BIGINT NOT NULL
);


-- i watch the data after i insert it..
SELECT * FROM events; 

-- Author : rub3ck0r3
