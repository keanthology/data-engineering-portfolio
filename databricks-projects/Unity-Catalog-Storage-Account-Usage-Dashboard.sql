-- DATASET 1 --
-- TOTAL SIZE IN TERABYTE --
SELECT table_catalog, table_schema, table_name, 
       --sum(file_size) /(1024 * 1024 * 1024 * 1024) AS file_size
       SUM(file_size) / POWER(1024, 4) AS file_size_tb
       --ROUND(SUM(file_size) / POWER(1000, 4), 8) AS file_size_tb
--FROM davi_analytics_wh_dev.dataset_uage.table_usage_history
FROM <catalog>.<schema>.<table-for-usage-history-monitoring>
WHERE table_catalog IS NOT NULL
GROUP BY table_catalog, table_schema, table_name
ORDER BY table_catalog

-- DATASET 2 --
-- SUM OF FILES IN CATALOG IN TB FORMAT --
-- PIE CHART --
SELECT  table_catalog,
        table_schema, 
        table_name,
        SUM(file_size) / POWER(1024, 4) AS file_size_tb
--FROM davi_analytics_wh_dev.dataset_uage.table_usage_history
FROM <catalog>.<schema>.<table-for-usage-history-monitoring>
--WHERE table_catalog IS NOT NULL
GROUP BY table_catalog,table_schema, table_name
ORDER BY table_catalog

-- DATASET 3 --
-- CATALOG TOTAL SIZE --
-- LINE GRAPH --
SELECT table_catalog, table_schema, table_name, 
       SUM(file_size) / POWER(1024, 4) AS file_size
--FROM davi_analytics_wh_dev.dataset_uage.table_usage_history
FROM <catalog>.<schema>.<table-for-usage-history-monitoring>
WHERE table_catalog IS NOT NULL
  AND table_catalog NOT IN ('summit_media')
  AND table_name NOT IN ('events', 'braze_events_users_behaviors_purchase_stg')
GROUP BY table_catalog, table_schema, table_name
ORDER BY file_size DESC

-- DATASE 4 --
-- SUM TABLE SIZE IN PIE CHART --
SELECT table_catalog,
       table_schema, 
       table_name, 
       table_type, 
       table_owner,
       last_altered_by,
       sum(file_size / POWER(1000, 4)) AS file_size_tb
    --    file_size / (1024 * 1024 * 1024) as file_size_gb
    --   SUM(file_size) / POWER(1024, 4) AS file_size_tb
--FROM davi_analytics_wh_dev.dataset_uage.table_usage_history
FROM <catalog>.<schema>.<table-for-usage-history-monitoring>
--WHERE table_catalog IS NOT NULL
GROUP BY table_catalog,table_schema, table_name, table_type, table_owner,last_altered_by
ORDER BY table_catalog
