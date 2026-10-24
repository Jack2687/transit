CREATE TABLE IF NOT EXISTS connection_health_channel_models (
 user_id text NOT NULL,
 admin_account_id text NOT NULL,
 target_id text NOT NULL,
 probe_model text NOT NULL DEFAULT '',
 quality_model text NOT NULL DEFAULT '',
 quality_revision text NOT NULL DEFAULT '',
 PRIMARY KEY (user_id, admin_account_id, target_id)
);
