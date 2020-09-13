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
import org.apache.kafka.common.network.Mode;
import org.apache.kafka.connect.mirror.MirrorMakerConfig;
import org.apache.kafka.connect.mirror.MirrorSourceConnector;
import org.apache.kafka.test.IntegrationTest;
import org.apache.kafka.test.TestSslUtils;
import org.apache.kafka.test.TestUtils;
import kafka.server.KafkaConfig$;

import org.junit.experimental.categories.Category;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Before;
import org.junit.Test;

/**
 * Tests MM2 replication with SSL enabled at backup kafka cluster
 */
@Category(IntegrationTest.class)
public class MirrorConnectorsIntegrationSSLTest extends MirrorConnectorsIntegrationBaseTest {

    private static final Logger log = LoggerFactory.getLogger(MirrorConnectorsIntegrationSSLTest.class);
    
    private static final List<Class> SOURCE_CONNECTOR = Arrays.asList(MirrorSourceConnector.class);

    @Before
    public void setup() throws InterruptedException {
        try {
            Map<String, Object> sslConfig = TestSslUtils.createSslConfig(false, true, Mode.SERVER, TestUtils.tempFile(), "testCert");
            backupBrokerProps.put(KafkaConfig$.MODULE$.ListenersProp(), "SSL://localhost:0");
            backupBrokerProps.put(KafkaConfig$.MODULE$.InterBrokerListenerNameProp(), "SSL");
            backupBrokerProps.putAll(sslConfig);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
        startClusters();
    }
    
    @Test
    public void testReplicationSSL() throws InterruptedException {
        produceRecords(primary, "test-topic-1");

        // one-way replication from primary -> backup
        mm2Props.put("backup->primary.enabled", "false");
        mm2Config = new MirrorMakerConfig(mm2Props);

        waitUntilMirrorMakerIsRunning(backup, SOURCE_CONNECTOR, mm2Config, "primary", "backup");
        
        Consumer<String, String> consumer = createSslConsumer(Collections.singletonMap(
                "group.id", "consumer-group-1"), "primary.test-topic-1");
        consumer.poll(Duration.ofMillis(500));
        consumer.commitSync();
        waitForConsumingAllRecords(consumer);
    }
}