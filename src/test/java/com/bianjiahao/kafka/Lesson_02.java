package com.bianjiahao.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class Lesson_02 {

    public static Properties initConfigProducer() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"0");
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"1.117.150.217:9092,1.117.150.217:9093");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.PARTITIONER_CLASS_CONFIG, DefaultPartitioner.class.getName());
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,"16384");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"0");
        properties.setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,"1048576");
        properties.setProperty(ProducerConfig.BUFFER_MEMORY_CONFIG,"33554432");
        properties.setProperty(ProducerConfig.MAX_BLOCK_MS_CONFIG,"60000");
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");
        properties.setProperty(ProducerConfig.SEND_BUFFER_CONFIG,"32768");
        properties.setProperty(ProducerConfig.RECEIVE_BUFFER_CONFIG,"32768");
        return properties;
    }

    public static void main(String[] args) {
        Properties properties = initConfigProducer();
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        while (true) {
            ProducerRecord<String, String> msg = new ProducerRecord<String, String>("demo","hello","word");
            Future<RecordMetadata> record = producer.send(msg);
            try {
                RecordMetadata recordMetadata = record.get();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }
}
