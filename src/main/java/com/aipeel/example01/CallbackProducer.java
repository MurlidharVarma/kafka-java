package com.aipeel.example01;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Properties;

import static io.restassured.RestAssured.get;

public class CallbackProducer {
    private static Logger LOG = LoggerFactory.getLogger(CallbackProducer.class);

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
        kafkaProducer.send(message, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                LOG.info("Sent: "+message.value()+" : Topic: "+recordMetadata.topic()+" Partition: "+recordMetadata.partition()+" Offset: "+recordMetadata.offset());
            }
        });

        // flush and close
        kafkaProducer.close();
    }

    private static String getRandomQuote(){
        List<Map> response = get("https://zenquotes.io/api/random").andReturn().body().jsonPath().getList("",Map.class);
        LOG.info("RESPONSE:" + response.get(0).get("q"));
        return (String)response.get(0).get("q");
    }
}
