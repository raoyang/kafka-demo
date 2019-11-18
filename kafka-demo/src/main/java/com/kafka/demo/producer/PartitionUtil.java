package com.kafka.demo.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

public class PartitionUtil implements Partitioner {

    // 分区数
    private static final Integer PARTITION_NUM = 3;

    @Override
    public int partition(String s, Object key, byte[] bytes, Object o1, byte[] bytes1, Cluster cluster) {
        if (null == key){
            return 0;
        }
        String keyValue = String.valueOf(key);
        // key取模
        int partitionId = (int) (Long.valueOf(keyValue) % PARTITION_NUM);

        return partitionId;
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> map) {

    }
}
