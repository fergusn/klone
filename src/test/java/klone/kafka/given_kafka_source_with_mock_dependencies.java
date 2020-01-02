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

import static org.junit.Assert.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyCollection;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import klone.kafka.Options;
import klone.api.Destination;
import klone.api.OffsetStore;

@ExtendWith(MockitoExtension.class)
public class given_kafka_source_with_mock_dependencies {
    @Mock OffsetStore store;
    @Mock OffsetStore.Transaction batch;
    @Mock Destination destination;
    @Mock Consumer<byte[], byte[]> consumer;
    @Captor ArgumentCaptor<ConsumerRebalanceListener> listener; 

    KafkaSource source;

    @BeforeEach public void given() {
        var options = new Options.Source();
        options.bootstrapServers = new String[] { "server" };
        options.topics = new String[] {"topic"};
        options.groupId = "abc";

        when(destination.offsetStore(any())).thenReturn(store);


        doNothing().when(consumer).subscribe(anyCollection(), listener.capture());

        source = new KafkaSource(consumer, destination, options);
    }

    @Test public void when_init_then_subscribe() {
        verify(consumer).subscribe(ArgumentMatchers.argThat((Collection<String> x) -> x.size() ==1 && x.contains("topic")), any(ConsumerRebalanceListener.class));
    }

    @Test public void when_parition_assigned_then_seek() throws Exception {
        var partition = new TopicPartition("topic", 10);
        when(store.offset("topic", 10)).thenReturn(Optional.of(99L));

        listener.getValue().onPartitionsAssigned(Set.of(partition));
        
        verify(consumer).seek(partition, 99);
    }

    @Test public void when_partition_assigned_without_saved_offset_then_seek_beginning() {
        var partition = new TopicPartition("topic", 10);
        when(store.offset(partition.topic(), partition.partition())).thenReturn(Optional.empty());

        listener.getValue().onPartitionsAssigned(Set.of(partition));
        
        verify(consumer).seekToBeginning(any());
    }

    @Test public void when_fetch_return_records_mapped() throws Exception {
        when(store.transaction()).thenReturn(batch);
        when(consumer.poll(any())).thenReturn(records(1));
        
        var actual = source.fetch(false);
        var message = actual.next();

        assertEquals("topic", message.topic);
        assertArrayEquals(new byte[] {1,2}, message.key);
        assertArrayEquals(new byte[] {3,4,5}, message.value);
    }

    @Test public void when_fetch_then_offset_updated() throws Exception {
        when(store.transaction()).thenReturn(batch);
        when(consumer.poll(any())).thenReturn(records(3));
        
        var actual = source.fetch(false);
        while (actual.hasNext()) {
            actual.next();
        }

        verify(batch).update("topic", 1, 1);
        verify(batch).update("topic", 1, 2);
        verify(batch).update("topic", 1, 3);
        verifyNoMoreInteractions(batch);
    }

    @Test public void when_close_then_close_consumer() throws Exception {
        source.close();

        verify(consumer).close();
    }


    private ConsumerRecords<byte[], byte[]> records(int n) {
        var xs =  IntStream.rangeClosed(1, n)
                           .mapToObj(i -> new ConsumerRecord<byte[], byte[]>("topic", 1, i, new byte[] {1,2}, new byte[] {3,4,5}))
                           .collect(Collectors.toList());
        return new ConsumerRecords<byte[], byte[]>(Map.of(new TopicPartition("topic", 1), xs));
    }

}