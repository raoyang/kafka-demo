package com.kafka.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.*;

@Service
public class ProducerDemo {


    static Properties properties = new Properties();

    static String topic = "test";

    static KafkaProducer<String, String> producer = null;

    // 核心池大小
    static int corePoolSize = 5;

    // 最大值
    static int maximumPoolSize = 20;

    // 无任务时存活时间
    static long keepAliveTime = 60;

    // 时间单位
    static TimeUnit timeUnit = TimeUnit.SECONDS;

    // 阻塞队列
    static BlockingQueue blockingQueue = new LinkedBlockingQueue();

    // 线程池
    static ExecutorService service = null;

    static{
        // 配置项
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.164.128:9092");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, PartitionUtil.class.getName());
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer<>(properties);
        // 初始化线程池
        service = new ThreadPoolExecutor(corePoolSize, maximumPoolSize, keepAliveTime, timeUnit, blockingQueue);
    }

    /**
     * 生产消息
     *
     * @return
     */
    public static ProducerThread createMsgTask() {
        OrderMessage orderMessage = new OrderMessage();
        orderMessage.setId(UUID.randomUUID().toString());
        long timestamp = System.currentTimeMillis();
        orderMessage.setCreateTime(timestamp);
        orderMessage.setRemake("rem");
        orderMessage.setsName("test");
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, timestamp + "", orderMessage.toString());
        ProducerThread task = new ProducerThread(producer, record);
        return task;
    }

    @PostConstruct
    private void init(){
        Thread t = new Thread(()->{
            while (true){
                try {
                    service.submit(createMsgTask());
                    Thread.sleep(500);
                }catch (Exception e){
                    e.printStackTrace();
                    break;
                }
            }
        });

        t.start();
    }

    public static void main(String args[]) throws Exception {
        //for (int i = 0; i < 6; i++) {
        while (true){
            service.submit(createMsgTask());
            Thread.sleep(1000);
        }
        //}
    }
}
