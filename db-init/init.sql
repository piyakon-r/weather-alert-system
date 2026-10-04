CREATE TABLE IF NOT EXISTS sensor_logs (
    id SERIAL PRIMARY KEY,
    location_id VARCHAR(50) NOT NULL,
    temperature NUMERIC(5, 2) NOT NULL,
    humidity NUMERIC(5, 2) NOT NULL,
    is_raining BOOLEAN NOT NULL,
    recorded_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_location_recorded ON sensor_logs(location_id, recorded_at DESC);