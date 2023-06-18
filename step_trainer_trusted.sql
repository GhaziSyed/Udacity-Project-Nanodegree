CREATE EXTERNAL TABLE IF NOT EXISTS `albis`.`trainer_trusted` (
  `sensorReadingTime` timestamp,
  `serialNumber` string,
  `distanceFromObject` string
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://albis-lake-house/step_trainer/trusted/'
TBLPROPERTIES ('classification' = 'json');
