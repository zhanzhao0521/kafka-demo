package com.estrus.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallBack {
    public static void main(String[] args) throws InterruptedException {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 500; i++) {

            kafkaProducer.send(new ProducerRecord<>("first", String.valueOf(i)), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        System.out.println("topic:"+ recordMetadata.topic()+" partition:"+ recordMetadata.partition());
                    }
                }
            });
            Thread.sleep(1);
        }
        kafkaProducer.close();
    }
}
