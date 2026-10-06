SET TimeZone='UTC';
-- Read-only against the compact archive index; no scratch paths required.
-- Native CMIPF band-months only. Change product explicitly for other products.
WITH months AS (
  SELECT strftime(month,'%Y-%m') AS month
  FROM generate_series(DATE '2026-01-01',DATE '2026-06-01',INTERVAL '1 month') AS dates(month)
), bands AS (SELECT unnest([1,2,3,7,8,9,10,13]) AS band), measured AS (
  SELECT month,band,sum(selected) AS selected,
         sum(greatest(checkpointed,completed)) AS accounted,
         sum(completed) AS verified,
         max(updated_at) AS updated_at,
         bool_and(status='complete') AS complete
  FROM tasks WHERE source='goes' AND product='ABI-L2-CMIPF'
    AND month BETWEEN '2026-01' AND '2026-06'
  GROUP BY ALL
)
SELECT months.month,bands.band,measured.selected,measured.accounted,measured.verified,
       round(100.0 * measured.accounted / nullif(measured.selected,0),1) AS percent,
       CASE WHEN measured.selected IS NULL THEN 'not inventoried / not started'
            WHEN measured.complete THEN 'verified' ELSE 'checkpointed / interrupted' END AS status,
       to_timestamp(measured.updated_at) AS updated_utc
FROM months CROSS JOIN bands LEFT JOIN measured USING(month,band)
ORDER BY month,band;
