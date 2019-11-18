package com.kafka.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

@Service
public class ConsumerDemo {

    static Properties properties = new Properties();

    static KafkaConsumer<String, String> consumer = null;

    static {
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.164.128:9092");
        properties.setProperty("group.id", "test");
        properties.setProperty("enable.auto.commit", "false");
        //properties.setProperty("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(properties);
        consumer.subscribe(Arrays.asList("test"));
    }

    //@PostConstruct
    private void init(){
        Thread t = new Thread(()->{
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("收到消息: offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
                }
                consumer.commitAsync();
            }
        });

        t.start();
    }

    public static void main(String args[]){
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("收到消息: offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
            consumer.commitAsync();
        }
    }
}
