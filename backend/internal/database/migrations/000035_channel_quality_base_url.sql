ALTER TABLE connection_health_channel_models
 ADD COLUMN IF NOT EXISTS quality_base_url text NOT NULL DEFAULT '';
