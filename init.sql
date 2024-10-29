-- Drop table if exists
DROP TABLE IF EXISTS health_records;

-- Create table
CREATE TABLE IF NOT EXISTS health_records (
    patient_id VARCHAR(100) NOT NULL,
    heart_rate DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    oxygen_saturation DOUBLE PRECISION NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    -- Composite primary key to ensure uniqueness of patient readings at a specific time
    PRIMARY KEY (patient_id, timestamp)
);

-- Create an index to improve query performance when searching by patient_id
CREATE INDEX IF NOT EXISTS idx_health_records_patient_id ON health_records(patient_id);

-- Create an index for timestamp queries
CREATE INDEX IF NOT EXISTS idx_health_records_timestamp ON health_records(timestamp);

-- Grant necessary permissions
-- Note: Replace 'health_user' with your actual database user if different
GRANT ALL PRIVILEGES ON TABLE health_records TO health_user;