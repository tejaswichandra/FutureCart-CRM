package com.edureka.futurecart.fact

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/** Description: create and load the pivot table "pivot_fact_futurecart_case_survey_dly"
 *                by joining fact table with all dimension table and applying the required transformations
 *@Author: Tejaswi Chandra
 */

object PivotFactCaseSurveyDaily extends FactSparkApp {

  def executeEtlProcess(spark: SparkSession): Unit = {
    import spark.implicits._

    val fc_qstn_df = spark.read.table("futurecart_dw.dim_futurecart_questions")
      .select(col("question_id"),
        col("question_desc"),
        col("response_type"),
        col("range"),
        expr("case when response_type='Scale' then int(split(negative_response_range,'-')[0]) end as negative_lower"),
        expr("case when response_type='Scale' then int(split(negative_response_range,'-')[1]) end as negative_upper"),
        expr("case when response_type='Scale' then int(split(neutral_response_range,'-')[0]) end as neutral_lower"),
        expr("case when response_type='Scale' then int(split(neutral_response_range,'-')[1]) end as neutral_upper"),
        expr("case when response_type='Scale' then int(split(positive_response_range,'-')[0]) end as positive_lower"),
        expr("case when response_type='Scale' then int(split(positive_response_range,'-')[1]) end as positive_upper")
      )

    val fact_case_srvy_df = spark.read.table("futurecart_dw.fact_futurecart_case_survey_dly")
    val dim_case_catg_df = spark.read.table("futurecart_dw.dim_futurecart_case_category")
    val dim_case_priority_df = spark.read.table("futurecart_dw.dim_futurecart_case_priority")
    val dim_emp_df = spark.read.table("futurecart_dw.dim_futurecart_employee")
    val dim_product_df = spark.read.table("futurecart_dw.dim_futurecart_product")
    val dim_call_center_df = spark.read.table("futurecart_dw.dim_futurecart_call_center")
    val dim_cal_df = spark.read.table("futurecart_dw.dim_futurecart_calendar")
    val dim_country_df = spark.read.table("futurecart_dw.dim_futurecart_country")
      .withColumn("alpha_2_upper", upper(col("alpha_2")))
      .withColumn("alpha_3_upper", upper(col("alpha_3")))


    val pivot_df = fact_case_srvy_df.join(dim_case_catg_df,
      fact_case_srvy_df("category") === dim_case_catg_df("category_key")
        && fact_case_srvy_df("sub_category") === dim_case_catg_df("sub_category_key"), "left")
      .join(dim_case_priority_df, dim_case_catg_df("priority") === dim_case_priority_df("priority_key"), "left")
      .join(dim_emp_df.as("emp_df"), fact_case_srvy_df("created_employee_key") === $"emp_df.emp_key", "left")
      .join(dim_emp_df.as("manager_df"), $"manager_df.emp_key" === $"emp_df.manager", "left")
      .join(dim_product_df, fact_case_srvy_df("product_code") === dim_product_df("product_id"), "left")
      .join(fc_qstn_df.as("q1_df"), $"q1_df.question_id" === lit("Q1"), "left")
      .join(fc_qstn_df.as("q2_df"), $"q2_df.question_id" === lit("Q2"), "left")
      .join(fc_qstn_df.as("q3_df"), $"q3_df.question_id" === lit("Q3"), "left")
      .join(fc_qstn_df.as("q5_df"), $"q5_df.question_id" === lit("Q5"), "left")
      .join(dim_cal_df.as("cal_open"), fact_case_srvy_df("case_create_date") === $"cal_open.calendar_date", "left")
      .join(dim_cal_df.as("cal_close"), fact_case_srvy_df("case_close_date") === $"cal_close.calendar_date", "left")
      .join(dim_call_center_df, fact_case_srvy_df("call_center_id") === dim_call_center_df("call_center_id"), "left")
      .join(dim_country_df.as("con1"), fact_case_srvy_df("country_cd") === $"con1.alpha_2_upper", "left")
      .join(dim_country_df.as("con2"), dim_call_center_df("country") === $"con2.alpha_2_upper", "left")
      .select(fact_case_srvy_df("case_no"),
        col("create_timestamp"),
        col("last_modified_timestamp"),
        col("status"),
        col("communication_mode"),
        col("case_close_date"),
        col("survey_date"),
        col("case_create_date"),
        dim_case_catg_df("priority"),
        dim_case_priority_df("priority").as("priority_details"),
        dim_case_priority_df("severity"),
        dim_case_priority_df("sla").as("sla_hours"),
        col("emp_df.first_name").as("employe_first_name"),
        col("emp_df.last_name").as("employe_last_name"),
        col("manager_df.first_name").as("manager_first_name"),
        col("manager_df.last_name").as("manager_last_name"),
        dim_product_df("department"),
        dim_product_df("brand"),
        dim_product_df("commodity_desc"),
        dim_product_df("sub_commodity_desc"),
        col("cal_open.week_name").as("case_open_week"),
        col("cal_open.month_name").as("case_open_month"),
        col("cal_open.quarter_name").as("case_open_quarter"),
        col("cal_close.week_name").as("case_close_week"),
        col("cal_close.month_name").as("case_close_month"),
        col("cal_close.quarter_name").as("case_close_quarter"),
        dim_call_center_df("call_center_vendor"),
        col("con2.name").as("call_center_country"),
        col("con1.name").as("case_country"),
        fact_case_srvy_df("Q1"),
        fact_case_srvy_df("Q2"),
        fact_case_srvy_df("Q3"),
        fact_case_srvy_df("Q4"),
        fact_case_srvy_df("Q5"),
        col("q1_df.negative_lower").as("q1_negative_lower"),
        col("q1_df.negative_upper").as("q1_negative_upper"),
        col("q1_df.neutral_lower").as("q1_neutral_lower"),
        col("q1_df.neutral_upper").as("q1_neutral_upper"),
        col("q1_df.positive_lower").as("q1_positive_lower"),
        col("q1_df.positive_upper").as("q1_positive_upper"),
        col("q2_df.negative_lower").as("q2_negative_lower"),
        col("q2_df.negative_upper").as("q2_negative_upper"),
        col("q2_df.neutral_lower").as("q2_neutral_lower"),
        col("q2_df.neutral_upper").as("q2_neutral_upper"),
        col("q2_df.positive_lower").as("q2_positive_lower"),
        col("q2_df.positive_upper").as("q2_positive_upper"),
        col("q3_df.negative_lower").as("q3_negative_lower"),
        col("q3_df.negative_upper").as("q3_negative_upper"),
        col("q3_df.neutral_lower").as("q3_neutral_lower"),
        col("q3_df.neutral_upper").as("q3_neutral_upper"),
        col("q3_df.positive_lower").as("q3_positive_lower"),
        col("q3_df.positive_upper").as("q3_positive_upper"),
        col("q5_df.negative_lower").as("q5_negative_lower"),
        col("q5_df.negative_upper").as("q5_negative_upper"),
        col("q5_df.neutral_lower").as("q5_neutral_lower"),
        col("q5_df.neutral_upper").as("q5_neutral_upper"),
        col("q5_df.positive_lower").as("q5_positive_lower"),
        col("q5_df.positive_upper").as("q5_positive_upper")
      )

    val pivot_final_df = pivot_df.select(
      col("case_no"),
      col("create_timestamp"),
      col("last_modified_timestamp"),
      col("status"),
      col("communication_mode"),
      col("case_close_date"),
      col("survey_date"),
      col("case_create_date"),
      col("priority"),
      col("priority_details"),
      col("severity"),
      col("sla_hours"),
      expr("case when status = 'Closed'\n  then int((unix_timestamp(last_modified_timestamp)-unix_timestamp(create_timestamp))/3600)" +
        "  else null end as hours_to_close"),
      col("employe_first_name"),
      col("employe_last_name"),
      col("manager_first_name"),
      col("manager_last_name"),
      col("department"),
      col("brand"),
      col("commodity_desc"),
      col("sub_commodity_desc"),
      expr("case  when Q1 >= q1_negative_lower and Q1 <= q1_negative_upper then 'Negative'" +
        "when Q1 >= q1_neutral_lower and Q1 <= q1_neutral_upper then 'Neutral' " +
        "when Q1 >= q1_positive_lower and Q1 <= q1_positive_upper then 'Positive' end as Support_Process_Feedback"),
      expr("case    when Q2 >= q2_negative_lower and Q2 <= q2_negative_upper then 'Negative'" +
        "   when Q2 >= q2_neutral_lower and Q2 <= q2_neutral_upper then 'Neutral'" +
        "   when Q2 >= q2_positive_lower and Q2 <= q2_positive_upper then 'Positive'    end as Employee_Conversation_Feedback"),
      expr("case    when Q3 >= q3_negative_lower and Q3 <= q3_negative_upper then 'Negative'" +
        "   when Q3 >= q3_neutral_lower and Q3 <= q3_neutral_upper then 'Neutral'" +
        "    when Q3 >= q3_positive_lower and Q3 <= q3_positive_upper then 'Positive'    end as Employee_Technical_Feedback"),
      expr("case  when Q4 = 'N' then 'Negative'    when Q4 = 'Y' then 'Positive'   end as Overall_Feedback"),
      expr("case  when Q5 >= q5_negative_lower and Q5 <= q5_negative_upper then 'Negative'" +
        "  when Q5 >= q5_neutral_lower and Q5 <= q5_neutral_upper then 'Neutral'" +
        "  when Q5 >= q5_positive_lower and Q5 <= q5_positive_upper then 'Positive'   end as Referral_Feedback"),
      col("case_open_week"),
      col("case_open_month"),
      col("case_open_quarter"),
      col("case_close_week"),
      col("case_close_month"),
      col("case_close_quarter"),
      col("call_center_vendor"),
      col("call_center_country"),
      col("case_country")
    )

    pivot_final_df.write.format("orc")
      .mode(SaveMode.Overwrite)
      .saveAsTable("futurecart_dw.pivot_fact_futurecart_case_survey_dly")
  }

  def main(args: Array[String]): Unit = {

    Console.println("start")

    spark.sql("set hive.exec.dynamic.partition=true")
    spark.sql("set hive.exec.dynamic.partition.mode=nonstrict")
    Console.println("session created")
    executeEtlProcess(spark)
  }


}
