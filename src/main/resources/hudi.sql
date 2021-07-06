-- -- 开启 mini-batch
-- SET table.exec.mini-batch.enabled=true;
-- -- mini-batch的时间间隔，即作业需要额外忍受的延迟
-- SET table.exec.mini-batch.allow-latency=1s;
-- -- 一个 mini-batch 中允许最多缓存的数据
-- SET table.exec.mini-batch.size=1000;
-- -- 开启 local-global 优化
-- SET table.optimizer.agg-phase-strategy=TWO_PHASE;
--
-- -- 开启 distinct agg 切分
-- SET table.optimizer.distinct-agg.split.enabled=true;

SET table.dynamic-table-options.enabled= true;
SET pipeline.name = hudi_sync1;
-- 1min
SET execution.checkpointing.interval=60000;
SET execution.checkpointing.min-pause=60000;

EXPORT topic=test;


CREATE TABLE IF NOT EXISTS default_catalog.default_database.`ods_finance_shipment_item_event_mws`
(
    `id`                                      BIGINT,
    `zid`                                     INT,
    `sid`                                     INT,
    `marketplace_id`                          STRING,
    `event_type`                              INT,
    `posted_date`                             STRING,
    `posted_datetime_locale`                  STRING,
    `posted_date_locale`                      STRING,
    `amazon_order_id`                         STRING,
    `seller_order_id`                         STRING,
    `shipment_event_id`                       INT,
    `seller_sku`                              STRING,
    `order_item_id`                           STRING,
    `order_adjustment_item_id`                STRING,
    `quantity_shipped`                        INT,
    `cost_of_points_granted_currency_code`    STRING,
    `cost_of_points_granted_currency_amount`  DECIMAL(10, 2),
    `cost_of_points_returned_currency_code`   STRING,
    `cost_of_points_returned_currency_amount` DECIMAL(10, 2),
    `gmt_modified`                            TIMESTAMP(3),
    `gmt_create`                              TIMESTAMP(3),
    `env_mark`                                STRING,
    PRIMARY KEY (`env_mark`, `id`) NOT ENFORCED
) with (
      'connector' = 'kafka',
      'topic' = '$topic',
      'properties.bootstrap.servers' = '10.50.17.51:9092',
      'properties.group.id' = 'hudi-oom1',
      'scan.startup.mode' = 'earliest-offset',
      'debezium-json.ignore-parse-errors' = 'true',
      'format' = 'debezium-json'
      );

-- DROP TABLE IF EXISTS hive_catalog.iceberg_db.`ods_finance_shipment_item_event_mws`;

CREATE TABLE IF NOT EXISTS default_catalog.default_database.`ods_finance_shipment_item_event_mws_hudi1`
(
    `year`  STRING,
    `month` STRING,
    `day`   STRING
)
    PARTITIONED BY (`year`,`month`,`day`)
with (
    'connector' = 'hudi',
    'table.type' = 'MERGE_ON_READ',
    'path' = 'hdfs:///hudi/ods_finance_charge_component_mws',
    'write.precombine.field' = 'gmt_modified',
    'write.tasks' = '10'
) LIKE default_catalog.default_database.`ods_finance_shipment_item_event_mws`(
    EXCLUDING ALL
    INCLUDING CONSTRAINTS
);





INSERT INTO default_catalog.default_database.`ods_finance_shipment_item_event_mws_hudi1`
SELECT *,
       CAST(YEAR(gmt_create) AS STRING) AS `year`,
       CAST(MONTH(gmt_create)  AS STRING) AS `month`,
       CAST(DAYOFMONTH(gmt_create) AS STRING) AS `day`
FROM default_catalog.default_database.`ods_finance_shipment_item_event_mws`;



./run.sh sql hudi hudi-oom1 -Dtaskmanager.memory.process.size=8192m
