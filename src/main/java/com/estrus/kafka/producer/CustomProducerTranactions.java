package com.estrus.kafka.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class CustomProducerTranactions {
    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG,"tranactional_id_01");
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<>(properties);

        kafkaProducer.initTransactions();
        kafkaProducer.beginTransaction();

        try {
            for (int i = 0; i < 5; i++) {

                kafkaProducer.send(new ProducerRecord<>("first",String.valueOf(i)));
            }
            int i= 1/0;
            kafkaProducer.commitTransaction();
        }catch (Exception e){
            kafkaProducer.abortTransaction();
        }finally {

            kafkaProducer.close();
        }


    }
}
