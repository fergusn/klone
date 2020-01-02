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
 * A source is an abstraction of a message store that allow fetching messages in batches. A source
 * is expected to participate in a Destination's transaction to allow exactly once semantics.
 */
public interface Source extends AutoCloseable {

    /**
     * 
     * @param drain Indicate that this is the last time that fetch will be invoked the source should cleanly shut down.
     * @return A batch of messages.
     */
    Batch fetch(Boolean drain) throws Exception;    
}