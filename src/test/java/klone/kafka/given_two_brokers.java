/*
Copyright 2019 Willem Ferguson.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package klone.kafka;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;

import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.log4j.ConsoleAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.wait.strategy.Wait;

import klone.Run;

@Tag("e2e")
public class given_two_brokers {
    static {
        final var console = new ConsoleAppender();
        final var PATTERN = "%d [%p|%c|%C{1}] %m%n";
        console.setLayout(new PatternLayout(PATTERN)); 
        console.setThreshold(Level.INFO);
        console.activateOptions();
        Logger.getRootLogger().addAppender(console);
    }

    public final KafkaContainer kafka1 = new KafkaContainer("5.2.1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .waitingFor(Wait.forListeningPort());

    public final KafkaContainer kafka2 = new KafkaContainer("5.2.1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
            .withEnv("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
            .waitingFor(Wait.forListeningPort());

    @Test
    public void when_replicate_topic_then_all_message_replicated() throws Exception {
        var t1 = "topic1";
        var t2 = "topic2";
        var n = 10_000;
        
        kafka1.start();
        kafka2.start();

        createTopic(kafka1, t1, 2);
        createTopic(kafka1, t2, 2);

        send(kafka1, t1, 1, n, x -> n % 2);     // Send n records to each topic 
        send(kafka1, t2, 1, n, x -> n % 2);

        final var thread1 = new Thread(() -> {
            Run.main(new String[] { 
                "kafka", "--bootstrap-server", kafka1.getBootstrapServers(), "--topic", t1, "--topic", t2, "--group-id", UUID.randomUUID().toString(), "--client-id", UUID.randomUUID().toString(),
                "kafka", "--bootstrap-server", kafka2.getBootstrapServers(),
            });
        });
        thread1.start();
        
        var props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka2.getBootstrapServers());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
        try (var consumer = new KafkaConsumer<byte[], byte[]>(props, new ByteArrayDeserializer(), new ByteArrayDeserializer())) {
            consumer.subscribe(List.of(t1, t2));

            var count = 0;
            while (true) {
                var records = consumer.poll(Duration.ofSeconds(5));
                count += records.count();
                if (count == 2 * n)  // we should receive n record on both topics
                    break;
            }

            var records = consumer.poll(Duration.ofSeconds(2));

            assertTrue(records.isEmpty());
        }
    }

    void send(KafkaContainer kafka, String topic, int value, int count, Function<Integer, Integer> partition) throws Exception {
        var props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (var producer = new KafkaProducer<byte[], byte[]>(props, new ByteArraySerializer(), new ByteArraySerializer())) {
            for (var i = 0; i < count; i++) {
                var part = partition.apply(value + i);
                var data = ByteBuffer.allocate(4).putInt(value + i).array();
                producer.send(new ProducerRecord<>(topic, part, data, data));
            }
            producer.flush();
        }
    }

    public void createTopic(KafkaContainer kafka, String name, Integer partitions) throws Exception {
        var props = new Properties();
        props.put(CommonClientConfigs.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());

        var client = KafkaAdminClient.create(props);

        client.createTopics(List.of(new NewTopic(name, partitions, (short) 1))).all().get();
    }
}