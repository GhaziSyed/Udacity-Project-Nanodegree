CREATE EXTERNAL TABLE IF NOT EXISTS `albis`.`customer_trusted` (
  `serialnumber` string,
  `sharewithpublicasofdate` bigint,
  `birthday` string,
  `registrationdate` string,
  `customername` string,
  `email` string,
  `lastupdatedate` bigint,
  `phone` string,
  `sharewithfriendsasofdate` bigint,
  `sharewithresearchasofdate` bigint
)
ROW FORMAT SERDE 'org.openx.data.jsonserde.JsonSerDe'
WITH SERDEPROPERTIES (
  'ignore.malformed.json' = 'FALSE',
  'dots.in.keys' = 'FALSE',
  'case.insensitive' = 'TRUE',
  'mapping' = 'TRUE'
)
STORED AS INPUTFORMAT 'org.apache.hadoop.mapred.TextInputFormat' OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
LOCATION 's3://albis-lake-house/customer/trusted/'
TBLPROPERTIES ('classification' = 'json');
