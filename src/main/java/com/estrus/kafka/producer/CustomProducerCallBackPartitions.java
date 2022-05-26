package com.estrus.kafka.producer;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerCallBackPartitions {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        //properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,"com.estrus.kafka.producer.MyPartitioner");

        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        for (int i = 0; i < 5; i++) {

            kafkaProducer.send(new ProducerRecord<>("first",1 ,"", "hello"+i), new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if(e == null){
                        System.out.println("topic:"+ recordMetadata.topic()+" partition:"+ recordMetadata.partition());
                    }
                }
            });
        }
        kafkaProducer.close();
    }
}
