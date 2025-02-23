CREATE TABLE IF NOT EXISTS run_metadata (
                run_id VARCHAR,
                pipeline_id VARCHAR,
                run_params VARCHAR,
                timestamp_inserted TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );