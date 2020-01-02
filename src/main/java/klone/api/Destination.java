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

/**
 * Destination is an abstraction of a target message store. A destination are required
 * to provide atomicity guarentees for a batch and provide and OffsetStore or Idempotence Store
 * to allow sources to commit progress.
 */
public interface Destination extends AutoCloseable {
    
    /**
     * Send a batch of messages atomically to a target store. Sources can 
     * request either a a OffsetStore or Indempotence  
     * @param batch the message batch to persist at the destination
     */
    void push(Batch batch);


    /**
     * An offset store allow sources to persist progress by commiting the log offset to the destination. 
     * @param id An identifier to unique identify the source instance
     * @return An instance of the OffsetStore that should be used throughout the lifetime of the source
     */
    OffsetStore offsetStore(String id);
}