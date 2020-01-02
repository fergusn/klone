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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.MetricsReporter;

import io.prometheus.client.Collector;
import io.prometheus.client.CollectorRegistry;

public class PrometheusReporter implements MetricsReporter {
    static final class Metric {
        public final KafkaMetric metric;
        public final List<String> labels;
        public final List<String> values;

        public Metric(KafkaMetric metric) {
            final var tags = metric.metricName().tags();
            
            this.metric = metric;
            this.labels = new ArrayList<>(tags.size());
            this.values = new ArrayList<>(tags.size());

            for (final var tag : tags.keySet()) {
                labels.add(tag.replace("-", "_"));
                values.add(tags.get(tag));
            }
        }
    }

    public class Adapter extends Collector  {
        private final String name;
        private final String description;
        private final Type type;
        private final List<Metric> metrics = new ArrayList<>(); 


        Adapter(final MetricName name) {
            this.name = prometheusName(name);
            this.description = name.description();
            this.type = Type.GAUGE;
        }

        void Add(KafkaMetric metric) {
            metrics.add(new Metric(metric));
        }

        @Override
        public List<MetricFamilySamples> collect() {
            final var samples = metrics.stream()
                                       .map(x -> new MetricFamilySamples.Sample(name, x.labels, x.values, x.metric.value()))
                                       .collect(Collectors.toList());

            return List.of(new MetricFamilySamples(name, type, description, samples));
        }   
    }

    @Override
    public void configure(Map<String, ?> configs) {
        
    }

    @Override
    public void init(List<KafkaMetric> metrics) {
        metrics.forEach(this::add);
    }

    final static ConcurrentHashMap<String, Adapter> metrics = new ConcurrentHashMap<>();
    final static ConcurrentHashMap<String, String> registry = new ConcurrentHashMap<>();

    @Override
    public void metricChange(KafkaMetric metric) {
        add(metric);
    }

    @Override
    public void metricRemoval(KafkaMetric metric) {
        final var name = prometheusName(metric.metricName());
        CollectorRegistry.defaultRegistry.unregister(metrics.remove(registry.remove(name)));
    }

    @Override
    public void close() {

    }

    private static String prometheusName(MetricName metricName) {
        return "kafka_client_" + metricName.group().replace("-", "_") + "_" + metricName.name().replace("-", "_");
    }

    private void add(KafkaMetric metric) {       
        final var name = prometheusName(metric.metricName());

        final var adapter = metrics.computeIfAbsent(name, x -> new Adapter(metric.metricName()));
        adapter.Add(metric);

        registry.computeIfAbsent(name, x-> {
            CollectorRegistry.defaultRegistry.register(adapter);
            return x;
        });

    }
}
