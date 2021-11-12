package com.aipeel.example01;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

public class SimpleConsumer {
    private static Logger LOG = LoggerFactory.getLogger(SimpleConsumer.class);

    static final private String KAFKA_SERVER = "localhost:9092";
    static final private String KAFKA_TOPIC = "piaTopic";
    static final private String CONSUMER_GROUP = "pia-group";

    public static void main(String[] args) {

        //Define properties
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, CONSUMER_GROUP);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");

        // Create Consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);

        //
        consumer.subscribe(Arrays.asList(KAFKA_TOPIC));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String, String> record: records){
                LOG.info("Partition: "+record.partition()+"|"+ record.offset() +" Key: "+record.key()+ " Value: "+record.value());
            }
        }
    }
}
