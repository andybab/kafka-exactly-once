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
package org.apache.kafka.connect.mirror.integration;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.MirrorCheckpointConnector;
import org.apache.kafka.connect.mirror.MirrorClient;
import org.apache.kafka.connect.mirror.MirrorHeartbeatConnector;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.connect.mirror.utils.ThreadedConsumer;
import org.apache.kafka.connect.mirror.utils.ThreadedProducer;
import org.apache.kafka.test.IntegrationTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.test.TestUtils.waitForCondition;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests MM2 replication and failover/failback logic.
 *
 * MM2 is configured with active/active replication between two Kafka clusters. Tests validate that
 * records sent to either cluster arrive at the other cluster. Then, a consumer group is migrated from
 * one cluster to the other and back. Tests validate that consumer offsets are translated and replicated
 * between clusters during this failover and failback.
 */
@Category(IntegrationTest.class)
public class MirrorConnectorsIntegrationTest extends MirrorConnectorsIntegrationBaseTest {

    private static final Logger log = LoggerFactory.getLogger(MirrorConnectorsIntegrationTest.class);
    
    private static final List<Class> CONNECTOR_LIST = 
            Arrays.asList(MirrorSourceConnector.class, MirrorCheckpointConnector.class, MirrorHeartbeatConnector.class);
    
    private static final List<Class> SOURCE_CONNECTOR = 
            Arrays.asList(MirrorSourceConnector.class);
    
    @Before
    public void setup() throws InterruptedException {
        Properties backupBrokerProps = new Properties();
        loadMMConfig();
        startClusters();
    }

    @Test
    public void testReplication() throws InterruptedException {
        produceRecords(primary, "test-topic-1");
        produceRecords(backup, "test-topic-1");
        // create consumers before starting the connectors so we don't need to wait for discovery
        Consumer<byte[], byte[]> consumer1 = primary.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
            "group.id", "consumer-group-1"), "test-topic-1", "backup.test-topic-1");
        consumer1.poll(Duration.ofMillis(500));
        consumer1.commitSync();
        consumer1.close();

        Consumer<byte[], byte[]> consumer2 = backup.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
            "group.id", "consumer-group-1"), "test-topic-1", "primary.test-topic-1");
        consumer2.poll(Duration.ofMillis(500));
        consumer2.commitSync();
        consumer2.close();

        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, "primary", "backup");

        waitUntilMirrorMakerIsRunning(primary, CONNECTOR_LIST, mm2Config, "backup", "primary");   

        MirrorClient primaryClient = new MirrorClient(mm2Config.clientConfig("primary"));
        MirrorClient backupClient = new MirrorClient(mm2Config.clientConfig("backup"));
        
        assertEquals("topic config was not synced", TopicConfig.CLEANUP_POLICY_COMPACT, 
                getTopicConfig(backup.kafka(), "primary.test-topic-1", TopicConfig.CLEANUP_POLICY_CONFIG));
        
        assertEquals("Records were not produced to primary cluster.", NUM_RECORDS_PRODUCED,
            primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count());
        assertEquals("Records were not replicated to backup cluster.", NUM_RECORDS_PRODUCED,
            backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "primary.test-topic-1").count());
        assertEquals("Records were not produced to backup cluster.", NUM_RECORDS_PRODUCED,
            backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-1").count());
        assertEquals("Records were not replicated to primary cluster.", NUM_RECORDS_PRODUCED,
            primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "backup.test-topic-1").count());
        assertEquals("Primary cluster doesn't have all records from both clusters.", NUM_RECORDS_PRODUCED * 2,
            primary.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, "backup.test-topic-1", "test-topic-1").count());
        assertEquals("Backup cluster doesn't have all records from both clusters.", NUM_RECORDS_PRODUCED * 2,
            backup.kafka().consume(NUM_RECORDS_PRODUCED * 2, RECORD_TRANSFER_DURATION_MS, "primary.test-topic-1", "test-topic-1").count());
        assertTrue("Heartbeats were not emitted to primary cluster.", primary.kafka().consume(1,
            RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0);
        assertTrue("Heartbeats were not emitted to backup cluster.", backup.kafka().consume(1,
            RECORD_TRANSFER_DURATION_MS, "heartbeats").count() > 0);
        assertTrue("Heartbeats were not replicated downstream to backup cluster.", backup.kafka().consume(1,
            RECORD_TRANSFER_DURATION_MS, "primary.heartbeats").count() > 0);
        assertTrue("Heartbeats were not replicated downstream to primary cluster.", primary.kafka().consume(1,
            RECORD_TRANSFER_DURATION_MS, "backup.heartbeats").count() > 0);
        assertTrue("Did not find upstream primary cluster.", backupClient.upstreamClusters().contains("primary"));
        assertEquals("Did not calculate replication hops correctly.", 1, backupClient.replicationHops("primary"));
        assertTrue("Did not find upstream backup cluster.", primaryClient.upstreamClusters().contains("backup"));
        assertEquals("Did not calculate replication hops correctly.", 1, primaryClient.replicationHops("backup"));
        assertTrue("Checkpoints were not emitted downstream to backup cluster.", backup.kafka().consume(1,
            CHECKPOINT_DURATION_MS, "primary.checkpoints.internal").count() > 0);

        Map<TopicPartition, OffsetAndMetadata> backupOffsets = backupClient.remoteConsumerOffsets("consumer-group-1", "primary",
            Duration.ofMillis(CHECKPOINT_DURATION_MS));

        assertTrue("Offsets not translated downstream to backup cluster. Found: " + backupOffsets, backupOffsets.containsKey(
            new TopicPartition("primary.test-topic-1", 0)));

        // Failover consumer group to backup cluster.
        consumer1 = backup.kafka().createConsumer(Collections.singletonMap("group.id", "consumer-group-1"));
        consumer1.assign(backupOffsets.keySet());
        backupOffsets.forEach(consumer1::seek);
        consumer1.poll(Duration.ofMillis(500));
        consumer1.commitSync();

        assertTrue("Consumer failedover to zero offset.", consumer1.position(new TopicPartition("primary.test-topic-1", 0)) > 0);
        assertTrue("Consumer failedover beyond expected offset.", consumer1.position(
            new TopicPartition("primary.test-topic-1", 0)) <= NUM_RECORDS_PRODUCED);
        assertTrue("Checkpoints were not emitted upstream to primary cluster.", primary.kafka().consume(1,
            CHECKPOINT_DURATION_MS, "backup.checkpoints.internal").count() > 0);

        consumer1.close();

        waitForCondition(() -> {
            try {
                return primaryClient.remoteConsumerOffsets("consumer-group-1", "backup",
                    Duration.ofMillis(CHECKPOINT_DURATION_MS)).containsKey(new TopicPartition("backup.test-topic-1", 0));
            } catch (Throwable e) {
                return false;
            }
        }, CHECKPOINT_DURATION_MS, "Offsets not translated downstream to primary cluster.");

        waitForCondition(() -> {
            try {
                return primaryClient.remoteConsumerOffsets("consumer-group-1", "backup",
                    Duration.ofMillis(CHECKPOINT_DURATION_MS)).containsKey(new TopicPartition("test-topic-1", 0));
            } catch (Throwable e) {
                return false;
            }
        }, CHECKPOINT_DURATION_MS, "Offsets not translated upstream to primary cluster.");

        Map<TopicPartition, OffsetAndMetadata> primaryOffsets = primaryClient.remoteConsumerOffsets("consumer-group-1", "backup",
                Duration.ofMillis(CHECKPOINT_DURATION_MS));
 
        // Failback consumer group to primary cluster
        consumer2 = primary.kafka().createConsumer(Collections.singletonMap("group.id", "consumer-group-1"));
        consumer2.assign(primaryOffsets.keySet());
        primaryOffsets.forEach(consumer2::seek);
        consumer2.poll(Duration.ofMillis(500));

        assertTrue("Consumer failedback to zero upstream offset.", consumer2.position(new TopicPartition("test-topic-1", 0)) > 0);
        assertTrue("Consumer failedback to zero downstream offset.", consumer2.position(new TopicPartition("backup.test-topic-1", 0)) > 0);
        assertTrue("Consumer failedback beyond expected upstream offset.", consumer2.position(
            new TopicPartition("test-topic-1", 0)) <= NUM_RECORDS_PRODUCED);
        assertTrue("Consumer failedback beyond expected downstream offset.", consumer2.position(
            new TopicPartition("backup.test-topic-1", 0)) <= NUM_RECORDS_PRODUCED);
        
        consumer2.close();
      
        // create more matching topics
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        backup.kafka().createTopic("test-topic-3", NUM_PARTITIONS);

        produceRecords(primary, "test-topic-2");
        produceRecords(backup, "test-topic-3");
        
        assertEquals("Records were not produced to primary cluster.", NUM_RECORDS_PRODUCED,
            primary.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-2").count());
        assertEquals("Records were not produced to backup cluster.", NUM_RECORDS_PRODUCED,
            backup.kafka().consume(NUM_RECORDS_PRODUCED, RECORD_TRANSFER_DURATION_MS, "test-topic-3").count());
 
        assertEquals("New topic was not replicated to primary cluster.", NUM_RECORDS_PRODUCED,
            primary.kafka().consume(NUM_RECORDS_PRODUCED, 2 * RECORD_TRANSFER_DURATION_MS, "backup.test-topic-3").count());
        assertEquals("New topic was not replicated to backup cluster.", NUM_RECORDS_PRODUCED,
            backup.kafka().consume(NUM_RECORDS_PRODUCED, 2 * RECORD_TRANSFER_DURATION_MS, "primary.test-topic-2").count());
    }
    
    @Test
    public void testOneWayReplicationWithAutoOffsetSync() throws InterruptedException {
        produceRecords(primary, "test-topic-1");
        // create consumers before starting the connectors so we don't need to wait for discovery
        try (Consumer<byte[], byte[]> consumer1 = primary.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "group.id", "consumer-group-1"), "test-topic-1")) {
            // we need to wait for consuming all the records for MM2 replicating the expected offsets
            waitForConsumingAllRecords(consumer1);
        }

        // enable automated consumer group offset sync
        mm2Props.put("sync.group.offsets.enabled", "true");
        mm2Props.put("sync.group.offsets.interval.seconds", "1");
        // one way replication from primary to backup
        mm2Props.put("backup->primary.enabled", "false");

        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, CONNECTOR_LIST, mm2Config, "primary", "backup");

        // create a consumer at backup cluster with same consumer group Id to consume 1 topic
        Consumer<byte[], byte[]> consumer = backup.kafka().createConsumerAndSubscribeTo(
            Collections.singletonMap("group.id", "consumer-group-1"), "primary.test-topic-1");

        waitForConsumerGroupOffsetSync(backup, consumer, Collections.singletonList("primary.test-topic-1"), 
                "consumer-group-1", NUM_RECORDS_PRODUCED);

        ConsumerRecords records = consumer.poll(Duration.ofMillis(500));

        // the size of consumer record should be zero, because the offsets of the same consumer group
        // have been automatically synchronized from primary to backup by the background job, so no
        // more records to consume from the replicated topic by the same consumer group at backup cluster
        assertEquals("consumer record size is not zero", 0, records.count());

        // now create a new topic in primary cluster
        primary.kafka().createTopic("test-topic-2", NUM_PARTITIONS);
        backup.kafka().createTopic("primary.test-topic-2", 1);
        // produce some records to the new topic in primary cluster
        produceRecords(primary, "test-topic-2");

        // create a consumer at primary cluster to consume the new topic
        try (Consumer<byte[], byte[]> consumer1 = primary.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
                "group.id", "consumer-group-1"), "test-topic-2")) {
            // we need to wait for consuming all the records for MM2 replicating the expected offsets
            waitForConsumingAllRecords(consumer1);
        }

        // create a consumer at backup cluster with same consumer group Id to consume old and new topic
        consumer = backup.kafka().createConsumerAndSubscribeTo(Collections.singletonMap(
            "group.id", "consumer-group-1"), "primary.test-topic-1", "primary.test-topic-2");

        waitForConsumerGroupOffsetSync(backup, consumer, Arrays.asList("primary.test-topic-1", "primary.test-topic-2"), 
                "consumer-group-1", NUM_RECORDS_PRODUCED);

        records = consumer.poll(Duration.ofMillis(500));
        // similar reasoning as above, no more records to consume by the same consumer group at backup cluster
        assertEquals("consumer record size is not zero", 0, records.count());
        consumer.close();
    }
    
    /*
     * This test is to validate MirrorSourceConnector follows "at most once" delivery guarantee
     * under broker restart / failure
     */
    @Test
    public void testWithBrokerRestart() throws InterruptedException {
        // test with a higher number of records
        int numRecords = NUM_RECORDS_PRODUCED * 8;
        // the sleep time between two produces used by background producer 
        int sleepMs = 50;
        int joinTimeoutMs = 1000;
        CountDownLatch producerLatch = new CountDownLatch(1);
        CountDownLatch consumerLatch = new CountDownLatch(1);
        // the purpose of background producer and consumer is to better test the failure case
        // to avoid serialization (produce -> broker restart -> consumer) by decoupling the
        // producer, embedded kafka and consumer. Since the consumer offsets are stored in 
        // kafka topic at backup cluster and offset commit is periodic, when kafka broker (at backup) 
        // restarts, duplicate records will be consumed, meaning "at most once" delivery guarantee 

        ThreadedProducer producerThread = new ThreadedProducer(primary.kafka().bootstrapServers(), 
                "test-topic-1", numRecords, sleepMs, producerLatch);
        producerThread.start();
        
        ThreadedConsumer consumerThread = new ThreadedConsumer(backup.kafka().bootstrapServers(),
                "primary.test-topic-1", "consumer-group-1", numRecords, false, consumerLatch);
        consumerThread.start();
        // one way replication from primary to backup
        mm2Props.put("backup->primary.enabled", "false");
        mm2Config = new MirrorMakerConfig(mm2Props);
       
        waitUntilMirrorMakerIsRunning(backup, SOURCE_CONNECTOR, mm2Config, "primary", "backup");
        log.trace("mm2 is ready for use");
        
        // sleep few seconds to let MM2 replicate some records for "end" consumer to consume them
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        // restart kafka broker at backup cluster
        restartKafkaBroker(backup);
        
        if (!producerLatch.await(3, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 3 minutes waiting for producer to finish");
        }
        
        if (!consumerLatch.await(3, TimeUnit.MINUTES)) {
            throw new TimeoutException("Timeout after 3 minutes waiting for consumer to finish, "
                    + "denoting some checks failed in ThreadConsumer. Enable trace-level logging to debug");
        }
    }
}
