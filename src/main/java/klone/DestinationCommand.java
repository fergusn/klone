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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import klone.api.Destination;
import picocli.CommandLine.ParentCommand;

public abstract class DestinationCommand implements Callable<Integer> {
    private static final Logger LOGGER = Logger.getLogger(DestinationCommand.class.getName());

    @ParentCommand SourceCommand source;

    protected abstract Destination create();

    @Override
    public Integer call() throws Exception {
        final AtomicBoolean cancelled = new AtomicBoolean(false);
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutdown signal received. Shutting down...");
            cancelled.compareAndSet(false, true);
        })); 

        DefaultExports.initialize();
        final var server = new HTTPServer(9000, true);

        try {
            final var destination  = create();
            final var source = this.source.create(destination);
        
            try {
                while (!cancelled.get()) {
                    destination.push(source.fetch(false));
                }
                
                // Shutdown requested - we need to drain in-flight messages and gracefuly shut down  
                destination.push(source.fetch(true));

            } catch (Exception ex) {
                LOGGER.log(Level.SEVERE, "Error while processing messages", ex);
                return 1;
            } finally {
                source.close();
                destination.close();
                server.stop();
            }
        } catch (Throwable ex) {
            LOGGER.log(Level.SEVERE, "Error while creating source and destination", ex);
            return 1;
        }

        return 0;        
    }
}