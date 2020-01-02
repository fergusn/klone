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

import java.util.Iterator;

import klone.api.Batch;
import klone.api.Message;

class StubBatch implements Batch {
    private Iterator<Message> it;

	public StubBatch(Iterable<Message> it) {
        super();
		this.it = it.iterator();
    }

	@Override
	public boolean hasNext() {
		return it.hasNext();
	}

	@Override
	public Message next() {
		return it.next();
	}

	@Override
	public void commit() throws Exception {
		
	}

	@Override
	public void abort() throws Exception {
		
	}

	@Override
	public void close() throws Exception {

	}

}