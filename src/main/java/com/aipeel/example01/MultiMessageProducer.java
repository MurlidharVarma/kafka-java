package com.aipeel.example01;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.restassured.RestAssured.get;

public class MultiMessageProducer {
    private static Logger LOG = LoggerFactory.getLogger(MultiMessageProducer.class);
    static final private String KAFKA_SERVER = "localhost:9092";
    static final private String KAFKA_TOPIC = "piaTopic";


    public static void main(String[] args) {
        //Define properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i=0; i<5; i++) {
            //Create ProducerRecord
            ProducerRecord<String, String> message = new ProducerRecord<String, String>(KAFKA_TOPIC,Integer.toString(i),getRandomWord());

            //Send Message to producer
            kafkaProducer.send(message);
        }

        // flush and close
        kafkaProducer.close();
    }

    private static String getRandomWord(){
        List<String> response = get("https://random-word-api.herokuapp.com/word?number=1").andReturn().body().jsonPath().getList("",String.class);
        LOG.info("RESPONSE:" + response.get(0));
        return (String)response.get(0);
    }
}
