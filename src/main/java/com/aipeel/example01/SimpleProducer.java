package com.aipeel.example01;

import static io.restassured.RestAssured.*;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Map;

public class SimpleProducer {
    private static Logger LOG = LoggerFactory.getLogger(SimpleProducer.class);

    static final private String KAFKA_SERVER = "localhost:9092";
    static final private String KAFKA_TOPIC = "leeTopic";


    public static void main(String[] args) {
        //Define properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());


        //Create Kafka Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<>(properties);

        //Create ProducerRecord
        ProducerRecord<String, String> message = new ProducerRecord<String, String>(KAFKA_TOPIC,getRandomQuote());

        //Send Message to producer
        kafkaProducer.send(message);

        // flush and close
        kafkaProducer.close();
    }

    private static String getRandomQuote(){
        List<Map> response = get("https://zenquotes.io/api/random").andReturn().body().jsonPath().getList("",Map.class);
        LOG.info("RESPONSE:" + response.get(0).get("q"));
        return (String)response.get(0).get("q");
    }
}

class ResponseBody{
    List<Map<String, String>> response;
}
