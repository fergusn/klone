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

import klone.SourceCommand;
import klone.api.Destination;
import klone.api.Source;
import picocli.CommandLine.Mixin;

@picocli.CommandLine.Command(name = "kafka", description = "Replicate messages from a Kafka topic(s)", commandListHeading = "Destinations:\n", customSynopsis = "klone kafka --bootstrap-server=<host:port> --group-id=<group-id> --topic=<topic> DESTINATION [options]")
public class KafkaSourceCommand extends SourceCommand {
    
    @Mixin Options.Source options;

	@Override
	public Source create(Destination destination) throws Exception {
		return KafkaSource.create(options, destination);
	}
}