package com.edureka.futurecart.stream.driver;

import com.edureka.futurecart.stream.serde.JsonSerializer;
import com.edureka.futurecart.stream.types.CaseRecord;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.util.Properties;

public class SalesProducer {
    public static final Logger logger = LogManager.getLogger(SalesProducer.class);
    public static void main(String[] args) {

//        if (args.length < 3) {
//            logger.info("please provide 3 arguments: topic_name, trasaction and sleeptime");
//            System.exit(0);
//        }
//        String topicName = args[0];
//        String transaction = args[1];
//        long sleeptime = Long.parseLong(args[2]);
//        logger.info("Provided arguments are:");
//        logger.info(String.format("topic name: %s", topicName));
//        logger.info(String.format("transaction type: %s", transaction));
//        logger.info(String.format("polltime: %d",sleeptime));


        logger.info("Creating Kafka Producer...");
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        String topicName = AppConfigs.topicName;

        KafkaProducer<Integer, CaseRecord> producer = new KafkaProducer<>(props);
        try {
            ObjectMapper mapper = new ObjectMapper();
            // JSON file to Java object
            CaseRecord[] e = mapper.readValue(new File("C:\\Users\\Tejaswi\\Desktop\\case.json"), CaseRecord[].class);
            logger.info("Initiating producer");
            int key =0;
            while (true) {

                for(int i=0; i < e.length; i++){
                    System.out.println("obj"+i);
                    System.out.println(e[i]);
                    producer.send(new ProducerRecord<>(topicName, key, e[i]));
                    key++;
                }
                    goToSleep();
            }
        } catch (Exception e) {
            logger.error(e);
        }finally {
            logger.info("Finished - Closing Kafka Producer.");
            producer.close();
        }

    }

    private static void goToSleep() {
        try {
            logger.info("sleeping for 45 seconds");
            Thread.sleep(45000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
