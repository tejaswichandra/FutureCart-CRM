package com.edureka.futurecart.stream.driver.cases;
import com.edureka.futurecart.stream.types.CaseRecord;
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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Properties;

public class CaseProducer {
    public static final Logger logger = LogManager.getLogger(com.edureka.futurecart.stream.driver.survey.SurveyProducer.class);

    public static void main(String[] args) {
        if (args.length < 4) {
            logger.error("please provide 4 arguments: topic_name, event, sleeptime, source_path for json files");
            System.exit(1);
        }
        String topicName = args[0];
        String event = args[1];
        long sleeptime = Long.parseLong(args[2]);
        String sourcepath = args[3];

        logger.info("Provided arguments are:");
        logger.info(String.format("topic name: %s", topicName));
        logger.info(String.format("event type: %s", event));
        logger.info(String.format("polltime: %d",sleeptime));
        logger.info(String.format("source path: %s",sourcepath));

        if (Files.isDirectory(Paths.get(sourcepath))){
            logger.info("source path exists, proceeding further!");
        }
        else{
            logger.error(String.format("source path: %s does not exists!!!", sourcepath));
            System.exit(1);
        }

        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        Session session = CassandraUtil.getCassandraSession();

        //String sourcepath = "/mnt/bigdatapgp/edureka_921625/project2/data/realtime/case";

        KafkaProducer<Integer, CaseRecord> producer = new KafkaProducer<>(props);


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
                        CaseRecord[] cr = new CaseRecord[0];
                        try {
                            cr = mapper.readValue(new File(filename), CaseRecord[].class);
                        } catch (IOException ex) {
                            ex.printStackTrace();
                        }
                        for (int i = 0; i < cr.length; i++) {
                            //System.out.println("obj" + i);
                            producer.send(new ProducerRecord<>(topicName, key, cr[i]));
                            key++;
                        }
                    }
                    logger.info(String.format("total no. of records sent to topic: %d", key));
                    //System.out.println(maxtimestamp);
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
