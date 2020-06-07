package com.edureka.futurecart.fact

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/** Description: create and load the fact table "futurecart_dw.fact_futurecart_case_survey_dly"
 *                by joining futurecart_case_dly & futurecart_survey_dly table
 *@Author: Tejaswi Chandra
 */

object FactCaseSurveyDaily extends FactSparkApp {

  def executeEtlProcess(spark: SparkSession): Unit = {

    val fc_case_dly_df = spark.read.table("futurecart_dw.futurecart_case_dly")

    val fc_survey_dly = spark.read.table("futurecart_dw.futurecart_survey_dly")
      .withColumn("rnk", row_number().over(Window.partitionBy("case_no").orderBy(asc("survey_timestamp"))))
      .filter(col("rnk") === 1)
      .select("survey_id", "case_no", "survey_timestamp", "q1", "q2", "q3", "q4", "q5", "survey_date")

    val case_survey_df = fc_case_dly_df.join(fc_survey_dly, fc_case_dly_df("case_no") === fc_survey_dly("case_no")
      && fc_case_dly_df("status") === lit("Closed"), "left")
      .select(fc_case_dly_df("case_no"),
        col("create_timestamp"),
        col("last_modified_timestamp"),
        col("created_employee_key"),
        col("call_center_id"),
        col("status"),
        col("category"),
        col("sub_category"),
        col("communication_mode"),
        col("country_cd"),
        col("product_code"),
        col("create_date"),
        col("close_date"),
        col("survey_timestamp"),
        col("q1"),
        col("q2"),
        col("q3"),
        col("q4"),
        col("q5"),
        col("survey_date"))
      .withColumnRenamed("create_date", "case_create_date")
      .withColumnRenamed("close_date", "case_close_date")
      .withColumn("rnk", row_number().over(Window.partitionBy("case_no").orderBy(asc("status"))))
      .filter(col("rnk") === 1)

    val case_survey_final_df = case_survey_df.select("case_no",
      "create_timestamp",
      "last_modified_timestamp",
      "created_employee_key",
      "call_center_id",
      "status",
      "category",
      "sub_category",
      "communication_mode",
      "country_cd",
      "product_code",
      "survey_timestamp",
      "q1",
      "q2",
      "q3",
      "q4",
      "q5",
      "case_close_date",
      "survey_date",
      "case_create_date")

    val createfactquery =
      """ CREATE TABLE if not exists futurecart_dw.fact_futurecart_case_survey_dly(
          case_no varchar(20),
          create_timestamp varchar(20),
          last_modified_timestamp varchar(20),
          created_employee_key varchar(50),
          call_center_id varchar(10),
          status varchar(10),
          category varchar(20),
          sub_category varchar(20),
          communication_mode varchar(10),
          country_cd varchar(10),
          product_code varchar(20),
          survey_timestamp varchar(20),
          q1 varchar(5),
          q2 varchar(5),
          q3 varchar(5),
          q4 varchar(5),
          q5 varchar(5),
          case_close_date string,
          survey_date string
          )
           partitioned by
           (  case_create_date string)
           stored as orc
             """

    spark.sql(createfactquery)
    case_survey_final_df.write.format("orc").mode(SaveMode.Overwrite).insertInto("futurecart_dw.fact_futurecart_case_survey_dly")

  }

  def main(args: Array[String]): Unit = {

    Console.println("start")

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    Console.println("session created")
    executeEtlProcess(spark)
  }


}
