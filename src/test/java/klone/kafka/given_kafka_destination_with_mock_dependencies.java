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

import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import klone.api.Batch;
import klone.api.Destination;
import klone.api.Message;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.*;

import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(MockitoExtension.class)
public class given_kafka_destination_with_mock_dependencies {
    @Mock Consumer<byte[], byte[]> consumer;
    @Mock Producer<byte[], byte[]> producer;
    @Mock Batch batch;

    private Destination destination;

    private final Message message = new Message("test", new byte[] { 0xa, 0xb }, new byte[] { 0xa, 0xb }); 

    @BeforeEach public void given() {
        var options = new Options.Destination();
        options.clientId = UUID.randomUUID().toString();
        options.transactionalId = UUID.randomUUID().toString();
        destination = new KafkaDestination(producer, x -> consumer, options);
    } 

    @Test public void when_init_then_init_transactions() {
        verify(producer).initTransactions();
    }

    @Test public void when_push_then_send_within_transaction() {
        when(batch.hasNext()).thenReturn(true, true, true, false);
        when(batch.next()).thenReturn(message);

        destination.push(batch);

        var order = inOrder(producer);

        order.verify(producer).beginTransaction();
        order.verify(producer, times(3)).send(any());        
        order.verify(producer).commitTransaction();
    }

    @Test public void when_push_then_commit_batch_within_transaction() throws Exception {
        when(batch.hasNext()).thenReturn(true, false);
        when(batch.next()).thenReturn(message);
        
        destination.push(batch);

        var order = inOrder(producer, batch);

        order.verify(producer).beginTransaction();
        order.verify(producer).send(any());        
        order.verify(batch).commit();
        order.verify(producer).commitTransaction();
    }

    @Test public void when_read_offset_then_get_offset_from_consume() {
        when(consumer.committed(new TopicPartition("topic", 3))).thenReturn(new OffsetAndMetadata(99));
        
        var store = destination.offsetStore("id");

        var offset = store.offset("topic", 3);

        assertEquals(99, offset.get());
    }

    @Test public void when_push_then_close_batch_after_commit() throws Exception{
        when(batch.hasNext()).thenReturn(true, false);
        when(batch.next()).thenReturn(message);
        
        destination.push(batch);

        var order = inOrder(producer, batch);

        order.verify(producer).commitTransaction();
        order.verify(batch).close();
    }

    @Test public void when_batch_comitted_then_last_offsets_commited() {
        var store = destination.offsetStore("gid");

        var tx = store.transaction();

        tx.update("aa", 2, 100);
        tx.update("aa", 2, 200);
        tx.update("aa", 3, 300);
        tx.update("bb", 4, 400);

        tx.commit();

        verify(producer).sendOffsetsToTransaction(argThat(x -> {
            return x.get(new TopicPartition("aa", 2)).offset() == 201 
                && x.get(new TopicPartition("aa", 3)).offset() == 301 
                && x.get(new TopicPartition("bb", 4)).offset() == 401;
        }), eq("gid"));;
    }
}
