package com.edureka.futurecart.stream.driver.survey;

import java.util.Properties;
import java.util.UUID;

import com.datastax.driver.core.Session;
import com.edureka.futurecart.stream.driver.AppConfigs;
import com.edureka.futurecart.stream.serde.AppSerdes;
import com.edureka.futurecart.stream.types.SurveyRecord;
import com.edureka.futurecart.stream.utility.CassandraUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class SurveyConsumer {
    public static final Logger logger = LogManager.getLogger(SurveyConsumer.class);

    public static void main(String[] args) {

        if (args.length < 1) {
            logger.info("please provide 1 argument: topic_name");
            System.exit(0);
        }
        String topicName = args[0];
        logger.info("Provided arguments are:");
        logger.info(String.format("topic name: %s", topicName));

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString());
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        Session session = CassandraUtil.getCassandraSession();

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, SurveyRecord> KS0 = builder.stream(topicName,
                Consumed.with(AppSerdes.Integer(), AppSerdes.SurveyRecord()));

        KS0.foreach((k, v) -> CassandraUtil.writeSurveyDatatoCassandra(session, k, v));

        Topology topology = builder.build();
        KafkaStreams streams = new KafkaStreams(topology, props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Stream");
            streams.close();
        }));

    }
}
