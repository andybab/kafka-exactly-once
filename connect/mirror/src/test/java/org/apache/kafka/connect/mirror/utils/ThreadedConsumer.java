/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.mirror.utils;

import java.util.Properties;

import static org.apache.kafka.connect.mirror.integration.MirrorConnectorsIntegrationBaseTest.NUM_PARTITIONS;
import static org.apache.kafka.connect.mirror.TestUtils.expectedRecords;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedConsumer extends Thread {
    private static final Logger log = LoggerFactory.getLogger(ThreadedConsumer.class);

    private final KafkaConsumer<String, String> consumer;
    private final String topic;
    private final String groupId;
    private final int numMessageToConsume;
    private final CountDownLatch latch;
    private final boolean exactlyOnce; // if false, it denotes "at most once" delivery guarantee 
    
    public ThreadedConsumer(final String brokerList,
            final String topic,
            final String groupId,
            final int numMessageToConsume,
            final boolean exactlyOnce,
            final CountDownLatch latch) {
        this.groupId = groupId;
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
 
        consumer = new KafkaConsumer<>(props);
        this.topic = topic;
        this.numMessageToConsume = numMessageToConsume;
        this.latch = latch;
        this.exactlyOnce = exactlyOnce;

        List<TopicPartition> tps = new ArrayList<>();
        for (int i = 0; i < NUM_PARTITIONS; i++) {
            tps.add(new TopicPartition(topic, i));
        }
        // manually assign partitions to consumer to save discovery time on partitions
        consumer.assign(tps);
    }

    @Override
    public void run() {
        long matchingCount = 0;
        Map<String, String> expectedRecords = expectedRecords(numMessageToConsume);
        Map<String, String> consumeMap = new HashMap<>();
        List<ConsumerRecord> consumeList = new ArrayList<>();
       
        while (expectedRecords.size() != matchingCount) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
            for (ConsumerRecord<String, String> record : records) {
                log.trace("key = {}, value = {}, partition = {}", record.key(), record.value(), record.partition());
                consumeMap.put(record.key(), record.value());
                consumeList.add(record);
            }
            matchingCount = expectedRecords.keySet().stream()
                    .filter(c -> consumeMap.containsKey(c) && consumeMap.get(c).equals(expectedRecords.get(c)))
                    .count();
            log.trace("consumeList size = {}, matchingCount = {}", consumeList.size(), matchingCount);
        }
        if (exactlyOnce) {
            // if "exactly once" delivery guarantee, the size of total consumed records should also 
            // equal to the size of expected records
            if (consumeList.size() == expectedRecords.size()) {
                latch.countDown();
            }
        }
        latch.countDown();
    }
}
