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

import static org.junit.Assert.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.when;
import static org.hamcrest.CoreMatchers.*;

import java.util.HashMap;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.KafkaMetric;
import org.apache.kafka.common.metrics.Measurable;
import org.apache.kafka.common.metrics.MetricConfig;
import org.apache.kafka.common.utils.Time;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import io.prometheus.client.CollectorRegistry;
import klone.kafka.PrometheusReporter;

@ExtendWith(MockitoExtension.class)
public class given_kafka_metrics {
    private MetricName name;
    private KafkaMetric metric;

    @Mock public Measurable measure;

    @BeforeEach public void given() {
        when(measure.measure(any(), anyLong())).thenReturn(123D);
        name = new MetricName("xname", "xgroup", "xdesc", new HashMap<>());
        metric = new KafkaMetric(new Object(), name, measure, new MetricConfig(), Time.SYSTEM);
    }

    @Test public void when_scrape_then_metric_published() {
        var reporter = new PrometheusReporter();
        reporter.metricChange(metric);

        var sample = (double) CollectorRegistry.defaultRegistry.getSampleValue("kafka_client_xgroup_xname");

        assertThat(sample, is(123D));
    }
} 