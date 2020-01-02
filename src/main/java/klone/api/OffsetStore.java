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
package klone.api;

import java.util.Optional;

/**
 * An offset store persist the position 
 */
public interface OffsetStore {

    /**
     * Get the current offset for a topic/partition. This is used by sources on startup 
     * and rebalancing.
     * @param topic the topic 
     * @param partition the partition
     * @return the last comitted position in the log or empty if no offset exists for the patition/topic
     */
    Optional<Long> offset(String topic, Integer partition);

    /**
     * Get the transaction context from a destination during the processing of a batch.
     * @return
     */
    Transaction transaction();

    /**
     * The transaction allow sources to participate in the destination's transaction. 
     */
    interface Transaction {

        /**
         * Update the last offset processed by the source. This is typically called while by the
         * source while iterating messages.
         * @param topic
         * @param partition
         * @param offset
         */
        void update(String topic, Integer partition, long offset);

        /**
         * Commit the offsets to the destination within the transaction. This is typically called 
         * in the Batch.commit
         */
        void commit();
    }
}