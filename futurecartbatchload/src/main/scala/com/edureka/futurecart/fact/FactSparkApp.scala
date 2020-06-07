package com.edureka.futurecart.fact

import org.apache.spark.sql.SparkSession

trait FactSparkApp {
  val spark: SparkSession = {
    SparkSession
      .builder
      .appName("futurecart_fact_load")
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
