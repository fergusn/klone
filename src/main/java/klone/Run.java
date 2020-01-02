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
package klone;

import java.util.concurrent.Callable;
import java.util.logging.Level;
import java.util.logging.Logger;

import klone.kafka.KafkaDestinationCommand;
import klone.kafka.KafkaSourceCommand;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Mixin;
import picocli.CommandLine.Spec;
import picocli.CommandLine.Model.CommandSpec;

@Command(name = "klone", description = "Replicate message from a source to a destination", commandListHeading = "Sources:\n", customSynopsis = "klone SOURCE [options] DESTINATION [options]")
public class Run implements Callable<Integer> {
    private static final Logger LOGGER = Logger.getLogger(Run.class.getName());

    public static void main(String[] args) {

        try {
            var cli = new CommandLine(Run.class)
                            .addSubcommand("kafka", new CommandLine(KafkaSourceCommand.class)
                                .addSubcommand("kafka", KafkaDestinationCommand.class));                                
                            
            final var results = cli.parseWithHandler(new CommandLine.RunLast(), args);
            final var status = (Integer) results.get(0);
            System.exit(status == null ? 1 : status);    
        } catch (Exception ex) {
            LOGGER.log(Level.SEVERE, "Critical error", ex);
            System.exit(1);
        }
    }

    @Mixin Options options = new Options();
    @Spec CommandSpec spec;

    @Override
    public Integer call() throws Exception {
        throw new CommandLine.ParameterException(spec.commandLine(), "Missing source and destination");
    }

}
