CREATE TABLE IF NOT EXISTS run_metadata (
  run_id VARCHAR,
  pipeline_id VARCHAR,
  run_params VARCHAR,
  ts_insert TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
);