package com.edureka.futurecart

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType

/** Description: load "futurecart_dw.futurecart_case_dly" table in hive from the corresponding cassandra table
  *                using spark-cassandra connector
  *@Author: Tejaswi Chandra
  */

object FCCaseDaily {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("futurecart_survey_daily")
      .config("spark.dynamicAllocation.enabled", "true")
      .config("spark.shuffle.service.enabled", "true")
      .config("spark.dynamicAllocation.initialExecutors", 1)
      .config("spark.dynamicAllocation.minExecutors", 1)
      .config("spark.dynamicAllocation.maxExecutors", 100)
      .config("spark.executor.cores", 3)
      .config("spark.network.timeout", "180s")
      .config("spark.sql.warehouse.dir", "/user/hive/warehouse/")
      .config("hive.metastore.uris", "thrift://ip-20-0-21-161.ec2.internal:9083,thrift://ip-20-0-21-85.ec2.internal:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")

    val tableDf = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map( "table" -> "fc_case_event", "keyspace" -> "edureka_921625"))
      .load()
    val stg_df = tableDf.select("case_no",
      "create_timestamp",
      "last_modified_timestamp",
      "created_employee_key",
      "call_center_id",
      "status",
      "category",
      "sub_category",
      "communication_mode",
      "country_cd",
      "product_code")
      .withColumn("row_insertion_dttm", current_timestamp())
      .withColumn("close_date", tableDf("last_modified_timestamp").cast(DateType))
      .withColumn("create_date", tableDf("create_timestamp").cast(DateType))


    stg_df.coalesce(1)
      .write
      .format("orc")
      .mode("overwrite")
      .insertInto("futurecart_dw.futurecart_case_dly")
  }
}
