
sqoop import --connect "jdbc:mysql://dbserver.edu.cloudlab.com/trainer_db" \
--username trainer \
--password  "root123" \
--query "select * from futurecart_case_details where \$CONDITIONS" \
--as-parquetfile \
--target-dir /tmp/edureka_921625/futurecart_case \
--m 1 \
--delete-target-dir

drop table if exists futurecart_dw.futurecart_case_dly_stg;
CREATE TABLE futurecart_dw.futurecart_case_dly_stg (
  `case_no` varchar(20),
  `create_timestamp` varchar(20),
  `last_modified_timestamp` varchar(20),
  `created_employee_key` varchar(50),
  `call_center_id` varchar(10),
  `status` varchar(10),
  `category` varchar(20),
  `sub_category` varchar(20),
  `communication_mode` varchar(10),
  `country_cd` varchar(10),
  `product_code` varchar(20)
) stored as parquet;

---Load imported data to stage table 
LOAD DATA  INPATH 'hdfs://nameservice1/tmp/edureka_921625/futurecart_case/' OVERWRITE INTO TABLE futurecart_dw.futurecart_case_dly_stg;

--- DDL to create partitioned target hive table
drop table if exists futurecart_dw.futurecart_case_dly;
CREATE TABLE futurecart_dw.futurecart_case_dly (
  `case_no` varchar(20),
  `create_timestamp` varchar(20),
  `last_modified_timestamp` varchar(20),
  `created_employee_key` varchar(50),
  `call_center_id` varchar(10),
  `status` varchar(10),
  `category` varchar(20),
  `sub_category` varchar(20),
  `communication_mode` varchar(10),
  `country_cd` varchar(10),
  `product_code` varchar(20),
  row_insertion_dttm string,
  close_date string
)
  partitioned by (create_date string)
  stored as orc;
  
--Script to load target dim_currency table from stage table
set hive.exec.dynamic.partition.mode=nonstrict;
insert overwrite table futurecart_dw.futurecart_case_dly partition(create_date)
select 
*,
current_timestamp() as row_insertion_dttm,
substring(last_modified_timestamp,0,10),
substring(create_timestamp,0,10)
from futurecart_dw.futurecart_case_dly_stg;
