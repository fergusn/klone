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

import java.util.Iterator;

/**
 * Sources batch messages to optimize replication throughput. The interface
 * also allow the <code>Destination</code> to coordinate transactions with the source.
 */
public interface Batch extends Iterator<Message> {

    /** 
     * Commit is invoked by the <code>Destination</code> before it commits the batch transaction.
     * It allows a Source to update the OffsetStore or IdempotenceStore within the Destinations transaction.
    */
    default void commit() throws Exception {};

    /**
     * Close is invoked by the Destination after it successfully comitted a transaction and allows 
     * a Sources to perform cleanup. 
     */
    default void close() throws Exception {};
    
    /**
     * Abort is invoked by destination after a commit failure and allows a Source to cleanly shut down. 
     */
    default void abort() throws Exception {};
    
    static Batch empty() {
		return new Batch(){
        
                @Override
                public Message next() {
                    return null;
                }
        
                @Override
                public boolean hasNext() {
                    Thread.onSpinWait();
                    return false;
                }
        };
	}
}