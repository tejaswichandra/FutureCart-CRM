package com.edureka.futurecart.stream.driver.survey;

import com.edureka.futurecart.stream.types.SurveyRecord;
import com.edureka.futurecart.stream.driver.AppConfigs;
import com.edureka.futurecart.stream.utility.CassandraUtil;
import com.edureka.futurecart.stream.serde.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.datastax.driver.core.Session;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Properties;

public class SurveyProducer {
    public static final Logger logger = LogManager.getLogger(SurveyProducer.class);

    public static void main(String[] args) {
        if (args.length < 3) {
            logger.info("please provide 3 arguments: topic_name, event and sleeptime");
            System.exit(0);
        }
        String topicName = args[0];
        String event = args[1];
        long sleeptime = Long.parseLong(args[2]);
        logger.info("Provided arguments are:");
        logger.info(String.format("topic name: %s", topicName));
        logger.info(String.format("event type: %s", event));
        logger.info(String.format("polltime: %d",sleeptime));

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        //String topicName = AppConfigs.topicName;
        //long sleeptime = 0;

        //String event = "survey";

        Session session = CassandraUtil.getCassandraSession();

        String sourcepath = "/mnt/bigdatapgp/edureka_921625/project2/data/realtime/survey";

        KafkaProducer<Integer, SurveyRecord> producer = new KafkaProducer<>(props);


        try {

            logger.info("Initiating producer");
            int key = 0;
            while (true) {

                logger.info(String.format("checking source path for latest json files for %s event:", event));

                File folder = new File(sourcepath);
                File[] listOfFiles = folder.listFiles();

                ArrayList<String> filelist = new ArrayList<String>();
                int maxtimestamp = 0;
                int last_updated_ts = CassandraUtil.fetchLastupdatedTimestamp(session, event);
                System.out.println("last updated timestamp:" + last_updated_ts);

                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        System.out.println(file.getName().split("\\.")[0].split("_")[2]);
                        System.out.println(file.getName());
                        int ts = Integer.parseInt(file.getName().split("\\.")[0].split("_")[2]);
                        if (ts > last_updated_ts) {
                            filelist.add(sourcepath + "/" + file.getName());
                            if (ts > maxtimestamp) {
                                maxtimestamp = ts;
                            }
                        }
                    }
                }

                if (filelist.isEmpty() == true) {
                    logger.info("no new files are generated after last fetch");
                } else {
                    for (String filename : filelist) {
                        System.out.println("reading data from: " + filename);
                        ObjectMapper mapper = new ObjectMapper();
                        SurveyRecord[] sr = new SurveyRecord[0];
                        try {
                            sr = mapper.readValue(new File(filename), SurveyRecord[].class);
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                        for (int i = 0; i < sr.length; i++) {
                            System.out.println("obj" + i);
                            System.out.println(sr[i].getSurveyTimestamp());
                            producer.send(new ProducerRecord<>(topicName, key, sr[i]));
                            key++;
                        }
                    }
                    System.out.println(maxtimestamp);
                    //logger.info(String.format("number of new records sent to the topic: %d", data.result.size()));
                    logger.info("updating db with last fetched timestamp");
                    CassandraUtil.updateLastupdatedTimestamp(session, event, maxtimestamp);

                }
                goToSleep(sleeptime);
            }
        } catch (Exception e) {
            logger.error(e);
        } finally {
            logger.info("Finished - Closing Kafka Producer.");
            producer.close();
        }


    }

    private static void goToSleep(Long sleeptime) {
        try {

            logger.info(String.format("sleeping for %d seconds", (sleeptime / 1000)));
            Thread.sleep(sleeptime);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}