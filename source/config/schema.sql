-- Unified Schema Definition for FSDS Patient Event Stream Database
-- Location: resources/database/schema.sql

-- 1. Core Subjects Directory Table
CREATE TABLE IF NOT EXISTS subjects (
    subject_id INTEGER PRIMARY KEY
);

-- 2. Granular Patient Event Log Table
-- Structured for high-throughput chronological replaying and change data capture (CDC)
CREATE TABLE IF NOT EXISTS patient_events (
    event_id SERIAL PRIMARY KEY,
    subject_id INTEGER REFERENCES subjects(subject_id) ON DELETE CASCADE,
    admission_id INTEGER,
    historical_time TIMESTAMP NOT NULL,
    ingested_at TIMESTAMP DEFAULT NOW(),
    code VARCHAR(100) NOT NULL,
    numeric_value DOUBLE PRECISION,
    text_value TEXT
);

-- 3. Optimized Database Indexes for Querying & Simulation Analytics
CREATE INDEX IF NOT EXISTS idx_patient_events_subject ON patient_events(subject_id);
CREATE INDEX IF NOT EXISTS idx_patient_events_hist_time ON patient_events(historical_time);
CREATE INDEX IF NOT EXISTS idx_patient_events_code ON patient_events(code);
