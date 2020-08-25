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

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.ConfigEntry;
import org.apache.kafka.clients.admin.DescribeConfigsResult;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.config.ConfigResource;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.utils.Exit;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.SourceAndTarget;
import org.apache.kafka.connect.util.clusters.EmbeddedConnectCluster;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;
import org.apache.kafka.test.IntegrationTest;
import org.junit.experimental.categories.Category;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.TimeUnit;

import static org.apache.kafka.connect.mirror.TestUtils.expectedRecords;
import static org.apache.kafka.connect.util.clusters.EmbeddedConnectClusterAssertions.CONNECTOR_SETUP_DURATION_MS;
import static org.apache.kafka.test.TestUtils.waitForCondition;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;
import org.junit.After;

/**
 * Common Test functions for MM2 integration tests
 */
@Category(IntegrationTest.class)
public class MirrorConnectorsIntegrationBaseTest {
    private static final Logger log = LoggerFactory.getLogger(MirrorConnectorsIntegrationBaseTest.class);

    protected static final int RECORD_CONSUME_DURATION_MS = 20_000;
    protected static final int OFFSET_SYNC_DURATION_MS = 30_000;
    protected static final int NUM_RECORDS_PRODUCED = 100;  // to save trees
    public static final int NUM_PARTITIONS = 10;
    protected static final int RECORD_TRANSFER_DURATION_MS = 20_000;
    protected static final int CHECKPOINT_DURATION_MS = 20_000;
    protected static final int NUM_WORKERS = 3;
    
    protected Map<String, String> mm2Props;
    protected MirrorMakerConfig mm2Config; 
    protected EmbeddedConnectCluster primary;
    protected EmbeddedConnectCluster backup;
    
    private final AtomicBoolean exited = new AtomicBoolean(false);
    private Properties primaryBrokerProps = new Properties();
    private Properties backupBrokerProps = new Properties();
    private Map<String, String> primaryWorkerProps;
    private Map<String, String> backupWorkerProps;
   
    protected void loadMMConfig() {
        mm2Props = basicMM2Config();
        mm2Config = new MirrorMakerConfig(mm2Props); 
    }
    
    protected void startClusters() throws InterruptedException {
        primaryBrokerProps.put("auto.create.topics.enable", "false");
        backupBrokerProps.put("auto.create.topics.enable", "false");
        
        primaryWorkerProps = mm2Config.workerConfig(new SourceAndTarget("backup", "primary"));
        backupWorkerProps = mm2Config.workerConfig(new SourceAndTarget("primary", "backup"));
                
        primary = new EmbeddedConnectCluster.Builder()
                .name("primary-connect-cluster")
                .numWorkers(3)
                .numBrokers(1)
                .brokerProps(primaryBrokerProps)
                .workerProps(primaryWorkerProps)
                .build();
        
        backup = new EmbeddedConnectCluster.Builder()
                .name("backup-connect-cluster")
                .numWorkers(3)
                .numBrokers(1)
                .brokerProps(backupBrokerProps)
                .workerProps(backupWorkerProps)
                .build();
        
        primary.start();
        primary.assertions().assertAtLeastNumWorkersAreUp(3,
                "Workers of primary-connect-cluster did not start in time.");
        
        backup.start();
        backup.assertions().assertAtLeastNumWorkersAreUp(3,
                "Workers of backup-connect-cluster did not start in time.");

        createTopics();
 
        log.info("primary REST service: {}", primary.endpointForResource("connectors"));
        log.info("backup REST service: {}", backup.endpointForResource("connectors"));
        log.info("primary brokers: {}", primary.kafka().bootstrapServers());
        log.info("backup brokers: {}", backup.kafka().bootstrapServers());
        
        // now that the brokers are running, we can finish setting up the Connectors
        mm2Props.put("primary.bootstrap.servers", primary.kafka().bootstrapServers());
        mm2Props.put("backup.bootstrap.servers", backup.kafka().bootstrapServers());
        
        Exit.setExitProcedure((status, errorCode) -> exited.set(true));
    }
    
    @After
    public void close() {
        for (String x : primary.connectors()) {
            primary.deleteConnector(x);
        }
        for (String x : backup.connectors()) {
            backup.deleteConnector(x);
        }
        deleteAllTopics(primary.kafka());
        deleteAllTopics(backup.kafka());
        primary.stop();
        backup.stop();
        try {
            assertFalse(exited.get());
        } finally {
            Exit.resetExitProcedure();
        }
    }
    /*
     * launch the connectors on kafka connect cluster and check if they are running
     */
    protected static void waitUntilMirrorMakerIsRunning(EmbeddedConnectCluster connectCluster, List<Class> connectorClasses,
            MirrorMakerConfig mm2Config, String primary, String backup) throws InterruptedException {
        for (int i = 0; i < connectorClasses.size(); i++) {
            String connector = connectorClasses.get(i).getSimpleName();
            Map<String, String> map = mm2Config.connectorBaseConfig(new SourceAndTarget(primary, backup), connectorClasses.get(i));
            log.info("map:");
            map.entrySet().stream().forEach(x -> log.info("{}, {}", x.getKey(), x.getValue()));
            String re = connectCluster.configureConnector(connector, mm2Config.connectorBaseConfig(new SourceAndTarget(primary, backup), 
                    connectorClasses.get(i)));
            log.info("response = {}", re);
            connectCluster.assertions().assertConnectorAndAtLeastNumTasksAreRunning(connector, 1,
                    "Connector " + connector + " tasks did not start in time on cluster: " + connectCluster);
        }
    }
 
    /*
     * delete all topics of the kafka cluster
     */
    protected static void deleteAllTopics(EmbeddedKafkaCluster cluster) {
        Admin client = cluster.createAdminClient();
        try {
            client.deleteTopics(client.listTopics().names().get());
        } catch (Throwable e) {
        }
    }
    
    /*
     * retrieve the config value given the kafka cluster, topic name and config name
     */
    protected static String getTopicConfig(EmbeddedKafkaCluster cluster, String topic, String configName) {
        Admin client = cluster.createAdminClient();
        Collection<ConfigResource> cr =  Collections.singleton(new ConfigResource(ConfigResource.Type.TOPIC, topic)); 
        try {
            DescribeConfigsResult configsResult = client.describeConfigs(cr);
            Config allConfigs = (Config) configsResult.all().get().values().toArray()[0];
            Iterator configIterator = allConfigs.entries().iterator();
            while (configIterator.hasNext()) {
                ConfigEntry currentConfig = (ConfigEntry) configIterator.next();     
                if (currentConfig.name().equals(configName)) {
                    return currentConfig.value();
                }
            }
        } catch (Throwable e) {
        }
        return null;
    }
    
    /*
     * for each pair of cluster and topic, produce given number of messages
     */
    protected static void produceRecords(EmbeddedConnectCluster clusters, String topic) {
        Map<String, String> recordSent = expectedRecords(NUM_RECORDS_PRODUCED);
        for (Map.Entry<String, String> entry : recordSent.entrySet()) {
            clusters.kafka().produce(topic, entry.getKey(), entry.getValue());
        }
    }
    
    /*
     * given consumer group, topics and expected number of records, make sure the consumer group offsets are eventually
     * synced to the expected offset numbers
     */
    protected static <T> void waitForConsumerGroupOffsetSync(EmbeddedConnectCluster connect, Consumer<T, T> consumer, 
            List<String> topics, String consumerGroupId, int numRecords) throws InterruptedException {
        Admin adminClient = connect.kafka().createAdminClient();
        List<TopicPartition> tps = new ArrayList<>(NUM_PARTITIONS * topics.size());
        for (int partitionIndex = 0; partitionIndex < NUM_PARTITIONS; partitionIndex++) {
            for (String topic : topics) {
                tps.add(new TopicPartition(topic, partitionIndex));
            }
        }
        long expectedTotalOffsets = numRecords * topics.size();

        waitForCondition(() -> {
            Map<TopicPartition, OffsetAndMetadata> consumerGroupOffsets =
                    adminClient.listConsumerGroupOffsets(consumerGroupId).partitionsToOffsetAndMetadata().get();
            long consumerGroupOffsetTotal = consumerGroupOffsets.values().stream().mapToLong(metadata -> metadata.offset()).sum();

            Map<TopicPartition, Long> offsets = consumer.endOffsets(tps, Duration.ofMillis(500));
            long totalOffsets = offsets.values().stream().mapToLong(l -> l).sum();

            // make sure the consumer group offsets are synced to expected number
            return totalOffsets == expectedTotalOffsets && consumerGroupOffsetTotal > 0;
        }, OFFSET_SYNC_DURATION_MS, "Consumer group offset sync is not complete in time");
    }

    /*
     * make sure the consumer to consume expected number of records
     */
    protected static void waitForConsumingAllRecords(Consumer<byte[], byte[]> consumer) throws InterruptedException {
        final AtomicInteger totalConsumedRecords = new AtomicInteger(0);
        waitForCondition(() -> {
            ConsumerRecords<byte[], byte[]> records = consumer.poll(Duration.ofMillis(500));
            return NUM_RECORDS_PRODUCED == totalConsumedRecords.addAndGet(records.count());
        }, RECORD_CONSUME_DURATION_MS, "Consumer cannot consume all records in time");
        consumer.commitSync();
    }
   
    /*
     * MM2 config to use in integration tests
     */
    protected static Map<String, String> basicMM2Config() {
        Map<String, String> mm2Props = new HashMap<>();
        mm2Props.put("clusters", "primary, backup");
        mm2Props.put("max.tasks", "10");
        mm2Props.put("topics", "test-topic-.*, primary.test-topic-.*, backup.test-topic-.*");
        mm2Props.put("groups", "consumer-group-.*");
        mm2Props.put("primary->backup.enabled", "true");
        mm2Props.put("backup->primary.enabled", "true");
        mm2Props.put("sync.topic.acls.enabled", "false");
        mm2Props.put("emit.checkpoints.interval.seconds", "1");
        mm2Props.put("emit.heartbeats.interval.seconds", "1");
        mm2Props.put("refresh.topics.interval.seconds", "1");
        mm2Props.put("refresh.groups.interval.seconds", "1");
        mm2Props.put("checkpoints.topic.replication.factor", "1");
        mm2Props.put("heartbeats.topic.replication.factor", "1");
        mm2Props.put("offset-syncs.topic.replication.factor", "1");
        mm2Props.put("config.storage.replication.factor", "1");
        mm2Props.put("offset.storage.replication.factor", "1");
        mm2Props.put("status.storage.replication.factor", "1");
        mm2Props.put("replication.factor", "1");
        
        return mm2Props;
    }
    
    /*
     * restart kafka broker and make sure it is successful
     */
    protected static void restartKafkaBroker(EmbeddedConnectCluster connect) throws InterruptedException {

        connect.kafka().stopOnlyKafka();
        log.info("issue kafka stop");
        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Group of workers did not remain the same after broker shutdown");

        // Allow for the workers to discover that the coordinator is unavailable, if the connector
        // is set up on this current EmbeddedConnectCluster
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        // Wait for the broker to be stopped
        assertTrue("Failed to stop kafka broker within " + CONNECTOR_SETUP_DURATION_MS + "ms",
                connect.kafka().runningBrokers().size() == 0);

        connect.kafka().startOnlyKafkaOnSamePorts();
        log.info("issue kafka start");
        // Allow for the kafka brokers to come back online
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        connect.assertions().assertExactlyNumWorkersAreUp(NUM_WORKERS,
                "Group of workers did not remain the same within the designated time.");

        // Allow for the workers to rebalance and reach a steady state
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        // Expect that the broker has started again
        assertTrue("Failed to start kafka broker within " + CONNECTOR_SETUP_DURATION_MS + "ms",
                connect.kafka().runningBrokers().size() > 0);
    }
    
    protected void createTopics() {
        // to verify topic config will be sync-ed across clusters
        Map<String, String> topicConfig = new HashMap<>();
        topicConfig.put(TopicConfig.CLEANUP_POLICY_CONFIG, TopicConfig.CLEANUP_POLICY_COMPACT);
        // create these topics before starting the connectors so we don't need to wait for discovery
        primary.kafka().createTopic("test-topic-1", NUM_PARTITIONS, 1, topicConfig);
        primary.kafka().createTopic("backup.test-topic-1", 1);
        primary.kafka().createTopic("heartbeats", 1);
        backup.kafka().createTopic("test-topic-1", NUM_PARTITIONS);
        backup.kafka().createTopic("primary.test-topic-1", 1);
        backup.kafka().createTopic("heartbeats", 1);
    }
}
