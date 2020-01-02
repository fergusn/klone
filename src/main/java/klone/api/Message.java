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

import org.apache.kafka.common.header.Header;

/**
 * The canonical representation of a message that is replicated from the source to the destination.
 */
public class Message {
    public String topic;
    public String id;
    public Iterable<Header> headers;
    public Long timestamp;
    public byte[] key;
    public byte[] value;

    public Message() { }

    public Message(String topic, byte[] key, byte[] value) { 
        this.topic = topic;
        this.key = key;
        this.value = value;
    }

    public static Header header(String key, byte[] value) {
        return new Header(){
        
            @Override
            public byte[] value() {
                return value;
            }
        
            @Override
            public String key() {
                return key;
            }
        };
    }
}
