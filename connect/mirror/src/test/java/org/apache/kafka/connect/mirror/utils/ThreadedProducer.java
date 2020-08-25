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

import static org.apache.kafka.connect.mirror.TestUtils.expectedRecords;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ThreadedProducer extends Thread {
    private static final Logger log = LoggerFactory.getLogger(ThreadedProducer.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;
    private int numRecords;
    private int sleepTimeMs = 0;
    private final CountDownLatch latch;

    public ThreadedProducer(final String brokerList, 
            final String topic,
            final int numRecords,
            final int sleepTimeMs,
            CountDownLatch latch) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "IdempotentProducer");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, 1);

        producer = new KafkaProducer<>(props);
        this.topic = topic;
        this.numRecords = numRecords;
        this.sleepTimeMs = sleepTimeMs;
        this.latch = latch;
    }
    
    @Override
    public void run() {
        Map<String, String> recordSent = expectedRecords(numRecords);
        for (Map.Entry<String, String> entry : recordSent.entrySet()) {
            // Send synchronously
            try {
                RecordMetadata meta = producer.send(new ProducerRecord<>(topic, entry.getKey(), entry.getValue())).get();
                log.trace("sent message : to topic {}, to partition {}, message key: {}, message value: {}, at offset {}", 
                        meta.topic(), meta.partition(), entry.getKey(), entry.getValue(), meta.offset());
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
            if (sleepTimeMs > 0) {
                try {
                    Thread.sleep(sleepTimeMs);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
        latch.countDown();
    }
}
