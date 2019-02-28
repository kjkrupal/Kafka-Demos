package com.github.krupal.kafka.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {

    public static void main(String[] args) throws Exception {

        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String server = "127.0.0.1:9092";

        // Create producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, server);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i = 0; i < 10; i++){

            String topic = "topic1";
            String value = "Hi my Kafka " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // Create record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            // Send data
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // Executes everytime a record is successfully sent or exception thrown

                    if(e == null){

                        logger.info("Recieved new metadata: \n" +
                                "Topic: " + recordMetadata.topic() + "\n" +
                                "Partition: " + recordMetadata.partition() + "\n" +
                                "Offset: " + recordMetadata.offset() + "\n" +
                                "Timestamp: " + recordMetadata.timestamp());

                    } else{
                        logger.error("Error while producing " + e);
                    }

                }
            }).get();
        }

        // Flush data
        producer.flush();
        producer.close();
    }

}
