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

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;
import java.util.function.Function;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.OutOfOrderSequenceException;
import org.apache.kafka.common.errors.ProducerFencedException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;

import klone.api.Batch;
import klone.api.Destination;
import klone.api.OffsetStore;

public class KafkaDestination implements Destination, Closeable {
    private static final Logger LOGGER = Logger.getLogger(KafkaDestination.class.getName());

    private final Producer<byte[], byte[]> producer;
    private final Function<Properties, Consumer<byte[], byte[]>> consumerFactory;
    private final Options.Destination options;

    KafkaDestination(final Producer<byte[], byte[]> producer, final Function<Properties, Consumer<byte[], byte[]>> consumerFactory, Options.Destination options) {
        this.producer = producer;
        this.consumerFactory = consumerFactory;
        this.options = options;

        producer.initTransactions();
    }

    public static Destination create(Options.Destination options) {
        final var props = options.properties(); 

        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.RETRIES_CONFIG, Integer.MAX_VALUE);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        props.put(ProducerConfig.METRIC_REPORTER_CLASSES_CONFIG, PrometheusReporter.class.getName());
        
        return new KafkaDestination(new KafkaProducer<>(props), KafkaConsumer::new, options);
    }

    @Override
    public void push(final Batch batch) {

        try {
            producer.beginTransaction();

            while (batch.hasNext()) {
                var record = batch.next();

                producer.send(new ProducerRecord<>(options.topic.getOrDefault(record.topic, record.topic), 
                                                   null, // partition,
                                                   record.timestamp, 
                                                   record.key, 
                                                   record.value, 
                                                   record.headers));

            }
            batch.commit();
            producer.commitTransaction();
            batch.close();
        } catch (final ProducerFencedException | OutOfOrderSequenceException | AuthorizationException e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw e;
        } catch (final KafkaException e) {
            producer.abortTransaction();
            // batch.abort();
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw e;
        } catch (final Exception e) {
            LOGGER.log(Level.SEVERE, e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws IOException {
        producer.close();
    }

    @Override
    public OffsetStore offsetStore(String id) {
        final var props = options.properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, props.getOrDefault(ConsumerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString()) + "-offsetstore");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, id);
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        final Consumer<byte[], byte[]> consumer = consumerFactory.apply(props);

        return new OffsetStore() {
        
            @Override
            public Optional<Long> offset(String topic, Integer partition) {
                try {
                    final var committed = consumer.committed(new TopicPartition(topic, partition));
                    return Optional.ofNullable(committed)
                                   .map(x -> x.offset());

                } catch (Exception ex) {
                    System.out.println(ex.toString());
                    return Optional.empty();
                }
            }
        
            @Override
            public Transaction transaction() {
                final var offsets = new HashMap<TopicPartition, OffsetAndMetadata>(); 

                return new Transaction() {

                    @Override
                    public void update(String topic, Integer partition, long offset) {
                        offsets.put(new TopicPartition(topic, partition), new OffsetAndMetadata(offset + 1));
                    }

                    @Override
                    public void commit() {
                        producer.sendOffsetsToTransaction(offsets, id);                
                    }
                };
            }
        };
    }
}
