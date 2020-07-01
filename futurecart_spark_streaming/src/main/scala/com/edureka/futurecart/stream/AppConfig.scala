package com.edureka.futurecart.stream

object AppConfig {
  val hostname = "dbserver.edu.cloudlab.com"
  val dbname = "trainer_db"
  val jdbcPort = 3306
  val username = "trainer"
  val password = "root123"
  val driver= "com.mysql.jdbc.Driver"
  val jdbc_url = "jdbc:mysql://%s:%s/%s?user=%s&password=%s".format(hostname, jdbcPort, dbname, username, password)

  val running_cases_kpis_loc = "running_cases_kpis_loc"
  val running_cases_kpis_priority_loc = "running_cases_kpis_priority_loc"

}
