
-- DDL to create stage table in Hive

  drop table if exists futurecart_dw.futurecart_survey_dly_stg ;
  CREATE TABLE futurecart_dw.futurecart_survey_dly_stg (
  `survey_id` varchar(20),
  `case_no` varchar(20),
  `survey_timestamp` varchar(20),
  `q1` varchar(5),
  `q2` varchar(5),
  `q3` varchar(5),
  `q4` varchar(5),
  `q5` varchar(5)
) 
  ROW FORMAT SERDE 
    'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
  WITH SERDEPROPERTIES ( 
    'field.delim'=',', 
    'serialization.format'=',') 
  STORED AS INPUTFORMAT 
    'org.apache.hadoop.mapred.TextInputFormat' 
  OUTPUTFORMAT 
    'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';
    
  
--sqoop command to import data from Mysql to existing stage hive table
sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/trainer_db" \
--username trainer \
--password  "root123" \
--query "select * from futurecart_case_survey_details where \$CONDITIONS" \
--target-dir /tmp/edureka_921625/survey\
--delete-target-dir \
--hive-import \
--hive-overwrite \
--hive-database futurecart_dw \
--hive-table futurecart_survey_dly_stg \
--hive-drop-import-delims \
--fields-terminated-by ',' \
--m 1


--- DDL to create partitioned target hive table
drop table if exists futurecart_dw.futurecart_survey_dly ;
CREATE TABLE futurecart_dw.futurecart_survey_dly (
    `survey_id` varchar(20),
    `case_no` varchar(20),
    `survey_timestamp` varchar(20),
    `q1` varchar(5),
    `q2` varchar(5),
    `q3` varchar(5),
    `q4` varchar(5),
    `q5` varchar(5),
    row_insertion_dttm string
  )
  partitioned by (survey_date string)
  stored as orc;
    
--Script to load target dim_currency table from stage table
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table futurecart_dw.futurecart_survey_dly partition(survey_date)
select 
*,
current_timestamp() as row_insertion_dttm,
substring(survey_timestamp,0,10)
from futurecart_dw.futurecart_survey_dly_stg;

