package com.bianjiahao.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.Future;

/**
 * @Author Obito
 * @Date 2022/5/6 20:32
 */
@SpringBootTest
public class Lesson_01 {

    @Test
    public void producer() throws Exception {
        // 指定Topic
        String topic = "demo";
        // 设置Producer的参数
        Properties p = new Properties();
        // 设置Kafka服务地址
        p.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"1.117.150.217:9092,1.117.150.217:9093");
        // 因为Kafka不参与数据的处理，所以我们要指定双方的编解码格式
        // 设置Key的序列化方式
        p.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // 设置Value的序列化方式
        p.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        // new一个消息生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(p);
        for (int i = 0; i < 3; i++) {
            for (int j = 0; j < 3; j++) {
                ProducerRecord<String, String> record = new ProducerRecord<>(topic,"item" + j,"val" + i);
                Future<RecordMetadata> send = producer.send(record);
                // 获取到消息发送的元数据
                RecordMetadata metadata = send.get();
                // 消息发送到了哪一个分区
                int partition = metadata.partition();
                // 当前分区的偏移量
                long offset = metadata.offset();
                System.out.println("key: " + record.key() + "val: " + record.value() + "partition: " + partition + "offset: " + offset);
            }
        }
    }

    @Test
    public void consumer() {
        // 指定Topic
        String topic = "demo";
        // 设置消费者属性
        Properties p = new Properties();
        // 设置Kafka服务器地址
        p.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"1.117.150.217:9092,1.117.150.217:9093");
        // 设置Key消费者反序列格式(应和生产者保持一致)
        p.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // 设置Value消费者反序列格式(应和生产者保持一致)
        p.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        // 设置消费组
        p.setProperty(ConsumerConfig.GROUP_ID_CONFIG,"Group-1");
        // 设置重置offset规则
        // 1、earliest：如果没有找到自己的offset，从partition的最开始一条信息开始消费
        // 2、latest：如果没有找到自己的offset，从partition之后的offset开始消费
        // 2、none：如果没有找到自己的offset，抛出异常
        p.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
        // 设置是否自动异步提交offset
        p.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        // 设置自动提交offset间隔时间，默认为5s
        p.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"15000");
        // 设置消费者一次性poll的最大数量
        p.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"200");
        // new一个消费者
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(p);
        consumer.subscribe(Collections.singletonList(topic), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(Collection<TopicPartition> collection) {
                System.out.println("----- onPartitionsRevoked -----");
                for (TopicPartition topicPartition : collection) {
                    System.out.println(topicPartition.partition());
                }
            }

            @Override
            public void onPartitionsAssigned(Collection<TopicPartition> collection) {
                // 知道自己消费的是那几个partition
                System.out.println("----- onPartitionsAssigned -----");
                for (TopicPartition topicPartition : collection) {
                    System.out.println(topicPartition.partition());
                }
            }
        });

        HashMap<TopicPartition, Long> mapForTime = new HashMap<>();
        Set<TopicPartition> topicPartitions = consumer.assignment();
        // 只有当poll的时候才会真正和Kafka建立连接
        while (topicPartitions.size() == 0) {
            consumer.poll(Duration.ofMillis(0));
            topicPartitions = consumer.assignment();
        }
        // 拿到分区信息后，填充时间戳
        for (TopicPartition topicPartition : topicPartitions) {
            mapForTime.put(topicPartition,1610629127300L);
        }
        // 拿到每个分区时间戳的offset
        Map<TopicPartition, OffsetAndTimestamp> offsetFotTime = consumer.offsetsForTimes(mapForTime);
        for (TopicPartition topicPartition : topicPartitions) {
            OffsetAndTimestamp offsetAndTimestamp = offsetFotTime.get(topicPartition);
            long offset = offsetAndTimestamp.offset();
            // 根据offset进行重定位
            consumer.seek(topicPartition,offset);
        }

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(0));
            if (!records.isEmpty()) {
                Set<TopicPartition> partitions = records.partitions();
                for (TopicPartition partition : partitions) {
                    List<ConsumerRecord<String, String>> partitionRecords = records.records(partition);
                    for (ConsumerRecord<String, String> partitionRecord : partitionRecords) {
                        int partitionOfRecord = partitionRecord.partition();
                        String key = partitionRecord.key();
                        String value = partitionRecord.value();
                        long offset = partitionRecord.offset();
                        long timestamp = partitionRecord.timestamp();
                        System.out.println("Partition: " + partitionOfRecord + "key: " + key + "value: " + value + "offset: " + offset + "timestamp: " + timestamp);
                        // 以每条消息的维度来提交offset
                        HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                        TopicPartition topicPartition = new TopicPartition(topic, partitionOfRecord);
                        OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
                        map.put(topicPartition, offsetAndMetadata);
                        consumer.commitSync(map);

                    }

                    // 以分区维度来提交offset，每个分区的数据消费完后提交offset
                    long offset = partitionRecords.get(partitionRecords.size() - 1).offset();
                    OffsetAndMetadata offsetAndMetadata = new OffsetAndMetadata(offset);
                    HashMap<TopicPartition, OffsetAndMetadata> map = new HashMap<>();
                    map.put(partition,offsetAndMetadata);
                    consumer.commitSync(map);
                }

                // 以批次的维度来提交offset
                consumer.commitSync();

            }
        }
    }

}
