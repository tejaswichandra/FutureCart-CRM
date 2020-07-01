package com.edureka.futurecart.stream

import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.functions.{expr, from_json, col, sum}
import org.apache.spark.sql.streaming.ProcessingTime

import scala.sys.process._


object CaseRunningKpi {

  def preProcess(): Unit = {
    val command = "hadoop fs -rm -r -f -skipTrash /user/edureka_921625/*"
    Process(command)!
  }


  def main(args: Array[String]): Unit = {

    val topic_name = args(0)

    preProcess()

    val spark = SparkSession
      .builder
      .appName("retail_cart_running_kpi")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    val case_json_schema = new StructType()
      .add("status", StringType)
      .add("category", StringType)
      .add("sub_category", StringType)
      .add("last_modified_timestamp", StringType)
      .add("case_no", StringType)
      .add("create_timestamp", StringType)
      .add("created_employee_key", StringType)
      .add("call_center_id", StringType)
      .add("product_code", StringType)
      .add("country_cd", StringType)
      .add("communication_mode", StringType)

//    createing read stream
    val stage_df = spark
    .readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "ip-20-0-31-221.ec2.internal:9092")
    .option("subscribe", topic_name)
    .load()
    .select(expr("CAST(value AS STRING)"))
    .withColumn("json", from_json(col("value"), case_json_schema))
      .select(col("json.status").alias("status"),
        col("json.category").alias("category_key"),
        col("json.sub_category").alias("sub_category_key")
    )

//   case KPIs
    val case_kpi_df = stage_df.select(
      expr("case when status = 'Open' then 1 else 0 end as total_case"),
      expr("case when status = 'Closed' then 1 else 0 end as closed_case"),
      expr("'Case_KPIs' as case")
    ).groupBy("case").agg(sum("total_case"), sum("closed_case"))
      .withColumnRenamed("sum(total_case)", "total_cases")
      .withColumnRenamed("sum(closed_case)","total_closed_cases")
      .withColumn("total_open_case", expr("total_cases - total_closed_cases"))


//    case KPIs by priority
    val query_p = "(select * from futurecart_case_priority_details) t1_alias"
    val priority_df = spark.read.format("jdbc")
      .option("driver", AC.driver)
      .option("url", AC.jdbc_url)
      .option("dbtable", query_p)
      .load()

    val query_cat = "(select * from futurecart_case_category_details) t1_alias"
    val category_df = spark.read.format("jdbc")
      .option("driver", AC.driver)
      .option("url", AC.jdbc_url)
      .option("dbtable", query_cat)
      .load()

    val priority_kpi_df = stage_df.join(category_df,Seq("category_key","sub_category_key"),"inner")
      .withColumnRenamed("priority","priority_key")
      .join(priority_df, Seq("priority_key"))
      .select(col("status"),
        col("priority"),
        col("severity"),
        expr("case when status = 'Open' then 1 else 0 end as total_case"))
      .groupBy("priority","severity").agg(sum("total_case"))
      .withColumnRenamed("sum(total_case)", "total_cases")

    val running_cases_kpis = case_kpi_df
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate","false")
      .option("numRows","10000")
      .option("checkpointLocation", "/user/edureka_921625/%s".format(AC.running_cases_kpis_loc))
      .trigger(ProcessingTime("20 seconds"))
      .start()

    val running_cases_kpis_by_priority = priority_kpi_df
      .writeStream
      .outputMode("complete")
      .format("console")
      .option("truncate","false")
      .option("numRows","10000")
      .option("checkpointLocation", "/user/edureka_921625/%s".format(AC.running_cases_kpis_priority_loc))
      .trigger(ProcessingTime("20 seconds"))
      .start()

    running_cases_kpis_by_priority.awaitTermination()
    running_cases_kpis.awaitTermination()

  }

}
