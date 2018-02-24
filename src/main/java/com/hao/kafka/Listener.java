package com.hao.kafka;

import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
//import javax.transaction.*;

public class Listener {
    public CountDownLatch countDownLatch0 = new CountDownLatch(20);
    public CountDownLatch countDownLatch1 = new CountDownLatch(20);
    public CountDownLatch countDownLatch2 = new CountDownLatch(20);

//    	@KafkaListener(id = "id0", topicPartitions = { @TopicPartition(topic = "SpringKafkaTopic", partitions = { "0" }) })
//    @KafkaListener(id = "id0", topics = "SpringKafkaTopic")
//    @KafkaListener(topics = "SpringKafkaTopic", group="t")
    @KafkaListener(topics = "SpringKafkaTopic")
//    @Transactional
    public void listenToptic1_1(ConsumerRecord<?, ?> record) {
        System.out.println("listenToptic1_1, Thread ID: " + Thread.currentThread().getId());
        System.out.println("Received: " + record + " Partition:" + record.partition());
        countDownLatch0.countDown();
    }

//    	@KafkaListener(id = "id1", topicPartitions = { @TopicPartition(topic = "SpringKafkaTopic", partitions = { "1","2" }) })
//    @KafkaListener(id = "id0", topics = "SpringKafkaTopic")
//    @KafkaListener(topics = "SpringKafkaTopic", group="t")
    @KafkaListener(topics = "SpringKafkaTopic")
//    @Transactional
    public void listenToptic1_2(ConsumerRecord<?, ?> record) {
        System.out.println("listenToptic1_2, Thread ID: " + Thread.currentThread().getId());
        System.out.println("Received: " + record + " Partition:" + record.partition());
        countDownLatch0.countDown();
    }

    //	@KafkaListener(id = "id1", topicPartitions = { @TopicPartition(topic = "SpringKafkaTopic2") })
//    @KafkaListener(id = "id1", topics = "SpringKafkaTopic2")
    @KafkaListener(topics = "SpringKafkaTopic2")
    public void listenToptic2(ConsumerRecord<?, ?> record) {
//        System.out.println("listenToptic2, Thread ID: " + Thread.currentThread().getId());
//        System.out.println("Received: " + record.value());
        countDownLatch1.countDown();
    }

    //	@KafkaListener(id = "id2", topicPartitions = { @TopicPartition(topic = "SpringKafkaTopic3") }
//    @KafkaListener(id = "id2", topics = "SpringKafkaTopic3")
    @KafkaListener(topics = "SpringKafkaTopic3")
    void listenTopic3(ConsumerRecord<?, ?> record) {
//        System.out.println("listenToptic3, Thread ID: " + Thread.currentThread().getId());
//        System.out.println("Received: " + record.value());
        countDownLatch2.countDown();
    }
}
