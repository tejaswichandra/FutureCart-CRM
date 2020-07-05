package com.edureka.futurecart.stream.utility;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.edureka.futurecart.stream.types.CaseRecord;
import com.edureka.futurecart.stream.types.SurveyRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class CassandraUtil {
    public static final Logger logger = LogManager.getLogger(CassandraUtil.class);
    public static Session getCassandraSession() {
//        String host="localhost";
//        String keyspace = "test_keyspace";
//        final Cluster cluster = Cluster.builder().addContactPoint(host)
//                .build();

        String host = "cassandradb.edu.cloudlab.com";
        String user="edureka_921625";
        String password = "edureka_9216255ar56";
        String keyspace = "edureka_921625";

        final Cluster cluster = Cluster.builder().addContactPoint(host).withCredentials(user, password)
                .build();
        final Session session = cluster.connect(keyspace);

        System.out.println("********* Cluster Information *************");
        System.out.println(" Cluster Name is: " + cluster.getClusterName());
        System.out.println(" Driver Version is: " + cluster.getDriverVersion());
        return session;

    }

    public static int fetchLastupdatedTimestamp(Session session, String transaction){
        String case_query = "select last_fetched_ts from fc_case_producer_audit;";
        String survey_query = "select last_fetched_ts from fc_survey_producer_audit;";
        String query = transaction.equals("case") ? case_query : survey_query;

        logger.info(String.format("fetching last updated timestamp for %s producer", transaction));
        ResultSet results = session.execute(query);
        int last_fetched_ts = 0;
        for (Row row : results) {
            last_fetched_ts = row.getInt("last_fetched_ts");
        }
        return last_fetched_ts;
    }
    public static void updateLastupdatedTimestamp(Session session, String transaction, int last_fetched_ts){
        String case_query = "INSERT INTO fc_case_producer_audit (id, last_fetched_ts) VALUES (?, ?)";
        String survey_query = "INSERT INTO fc_survey_producer_audit (id, last_fetched_ts) VALUES (?, ?)";
        String query = transaction.equals("case") ? case_query : survey_query;

        logger.info(String.format("updating last_fetched_timestamp for %s producer to: %d", transaction, last_fetched_ts));
        session.execute(query,  1, last_fetched_ts);

    }
    public static void writeCaseDatatoCassandra(Session session, int key, CaseRecord cr){
        System.out.println(String.format("%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s , %s", key, cr.getStatus(),
                cr.getCategory(), cr.getSubCategory(), cr.getLastModifiedTimestamp(), cr.getCaseNo(),
                cr.getCreateTimestamp(), cr.getCreatedEmployeeKey(), cr.getCallCenterId(), cr.getProductCode(),
                cr.getCountryCd(), cr.getCommunicationMode()));
        session.execute("insert into fc_case_event (key, status, category, sub_category, last_modified_timestamp, " +
                        "case_no, create_timestamp, created_employee_key, call_center_id, product_code, " +
                        "country_cd, communication_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", key, cr.getStatus(), cr.getCategory(), cr.getSubCategory(),
                cr.getLastModifiedTimestamp(), cr.getCaseNo(), cr.getCreateTimestamp(), cr.getCreatedEmployeeKey(),
                cr.getCallCenterId(), cr.getProductCode(), cr.getCountryCd(), cr.getCommunicationMode());
    }
    public static void writeSurveyDatatoCassandra(Session session, int key, SurveyRecord sr){
        System.out.println(String.format("%s, %s, %s, %s, %s, %s, %s, %s, %s", key, sr.getQ1(), sr.getQ3(),sr.getQ2(), sr.getQ5(),
                sr.getQ4(), sr.getCaseNo(), sr.getSurveyTimestamp(), sr.getSurveyId()));
        session.execute("insert into fc_survey_event (key, Q1, Q3, Q2, Q5, Q4, case_no, survey_timestamp, survey_id) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                key, sr.getQ1(), sr.getQ3(),sr.getQ2(), sr.getQ5(), sr.getQ4(), sr.getCaseNo(), sr.getSurveyTimestamp(),
                sr.getSurveyId());
    }

}
