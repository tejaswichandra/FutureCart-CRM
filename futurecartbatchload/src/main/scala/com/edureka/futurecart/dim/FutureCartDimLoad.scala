package com.edureka.futurecart.dim

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/** Description: load hive table from MySQL using Spark JDBC
 * Arguments: MySQL_source_table HIVE_Target_table
 *@Author: Tejaswi Chandra
 */

trait DimSparkApp {
  val spark: SparkSession = {

    SparkSession
      .builder
      .appName("futurecart_dim_load")
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
  }
}

object FutureCartDimLoad extends DimSparkApp {

  def main(args: Array[String]): Unit = {

    val source_table = args(0)
    val target_table = args(1)
    val hostname = "dbserver.edu.cloudlab.com"
    val dbname = "trainer_db"
    val jdbcPort = 3306
    val username = "trainer"
    val password = "root123"
    //s"(select * from ${source_table}) t1_alias"
    val jdbc_url = "jdbc:mysql://%s:%s/%s?user=%s&password=%s".format(hostname, jdbcPort, dbname, username, password)
    val query = "(select * from %s) t1_alias".format(source_table)

    //    logger.info("MySql source table: %s".format(source_table))
    //    logger.info("Hive target table: %s".format(target_table))
    //    logger.info("jdbc url: %s".format(jdbc_url))


    val stg_df = spark.read.format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", query)
      .load()

    stg_df.withColumn("row_insertion_dttm", current_timestamp())
      .coalesce(1)
      .write
      .format("orc")
      .mode("overwrite")
      .saveAsTable(target_table)
  }

}
