-- Read-only typed archive tasks. No scratch files or source-object inventories.
-- SQL status is recorded state; the CLI also checks process identity.
SET TimeZone='UTC';
SELECT source,product,month,band,selected,
       greatest(checkpointed,completed) AS accounted,
       completed AS verified,
       round(100.0 * greatest(checkpointed,completed)/nullif(selected,0),1) AS percent,
       status,
       to_timestamp(try_cast(json_extract_string(metrics_json,'$.attempt_started_at') AS DOUBLE)) AS started_utc,
       to_timestamp(updated_at) AS updated_utc,
       task_id AS archive
FROM tasks
WHERE task_id LIKE '%raw.zarr.zip'
ORDER BY month,product,band,task_id;
