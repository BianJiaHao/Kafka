package com.bianjiahao.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * @Author Obito
 * @Date 2022/5/6 20:32
 */
@SpringBootTest
public class Test01 {

    @Test
    public void producer() throws Exception {
        String topic = "demo";
        Properties p = new Properties();
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"1.117.150.217:9092,1.117.150.217:9093");
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);

        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic,"item" + j,"val" + i);
                Future<RecordMetadata> send = producer.send(record);
                RecordMetadata metadata = send.get();
                int partition = metadata.partition();
                long offset = metadata.offset();
                System.out.println("key: " + record.key() + "val: " + record.value() + "partition: " + partition + "offset: " + offset);
            }
        }
    }

}
