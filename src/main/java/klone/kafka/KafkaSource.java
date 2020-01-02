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

import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import klone.api.Source;
import klone.api.Batch;
import klone.api.Destination;
import klone.api.Message;
import klone.api.OffsetStore;

public class KafkaSource implements Source {
    private static final Logger LOGGER = Logger.getLogger(KafkaSource.class.getName());


    private final Consumer<byte[], byte[]> consumer;
    private final OffsetStore offsetStore;
    private final Map<String, String> topics = new HashMap<>();

    KafkaSource(final Consumer<byte[], byte[]> consumer, final Destination destination, final Options.Source options) {
        this.consumer = consumer;
        this.offsetStore = destination.offsetStore(options.groupId);

        for (var topic : options.topics) {
            if (topic.contains("=")) {
                var parts = topic.split("=");
                topics.put(parts[0], parts[1]);
            } else {
                topics.put(topic, topic);
            }
        } 

        consumer.subscribe(topics.keySet(), new ConsumerRebalanceListener() {
            @Override
            public void onPartitionsRevoked(final Collection<TopicPartition> partitions) { }

            @Override
            public void onPartitionsAssigned(final Collection<TopicPartition> partitions) {
                for (final var partition : partitions) {
                    offsetStore.offset(topics.get(partition.topic()), partition.partition())
                               .ifPresentOrElse(x  -> seek(partition, x), () -> begin(partition));
                }
            }

            private void seek(TopicPartition partition, Long offset) {
                LOGGER.info(String.format("Setting offset for partition %s-%d to offset %d.", partition.topic(), partition.partition(), offset));
                consumer.seek(partition, offset);
            }

            private void begin(TopicPartition partition) {
                LOGGER.info(String.format("Offset for partition %s-%d not found. Seeking to beginning.", partition.topic(), partition.partition()));
                consumer.seekToBeginning(Set.of(partition));
            }

        });
    }

    public static Source create(final Options.Source options, final Destination destination) {
        final var props = options.properties();

        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        // batch: props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, value)
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG, PrometheusReporter.class.getName());

        return new KafkaSource(new KafkaConsumer<>(props), destination, options);
    }

    @Override
    public Batch fetch(Boolean drain) throws Exception {
        final var records = consumer.poll(Duration.ofSeconds(2));

        if (records.isEmpty()) {
            return Batch.empty();
        }

        final var offsets = offsetStore.transaction();
        final var iterator = records.iterator();

        return new Batch() {
            @Override
            public boolean hasNext() {
                return iterator.hasNext();
            }

            @Override
            public Message next() {
                final var record = iterator.next();

                final var message = new Message();
                message.topic = topics.get(record.topic());
                message.timestamp = record.timestamp();
                message.headers = record.headers();
                message.key = record.key();
                message.value = record.value();

                offsets.update(message.topic, record.partition(), record.offset());

                return message;
            }
            
            @Override
            public void commit() throws Exception { 
                offsets.commit();
            }
        };
    }

    @Override
    public void close() throws IOException {
        consumer.close();
    }
}
