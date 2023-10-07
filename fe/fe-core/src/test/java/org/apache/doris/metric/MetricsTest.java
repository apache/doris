// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.doris.metric;

import org.apache.doris.common.FeConstants;

import com.codahale.metrics.Histogram;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.List;
import java.util.Map;
import java.util.SortedMap;

public class MetricsTest {

    @BeforeClass
    public static void setUp() {
        FeConstants.runningUnitTest = true;
        MetricRepo.init();
    }

    @Test
    public void testTcpMetrics() {
        List<Metric> metrics = MetricRepo.getMetricsByName("snmp");
        Assert.assertEquals(4, metrics.size());
        for (Metric metric : metrics) {
            GaugeMetric<Long> gm = (GaugeMetric<Long>) metric;
            String metricName = gm.getLabels().get(0).getValue();
            if (metricName.equals("tcp_retrans_segs")) {
                Assert.assertEquals(Long.valueOf(826271L), (Long) gm.getValue());
            } else if (metricName.equals("tcp_in_errs")) {
                Assert.assertEquals(Long.valueOf(12712L), (Long) gm.getValue());
            } else if (metricName.equals("tcp_in_segs")) {
                Assert.assertEquals(Long.valueOf(1034019111L), (Long) gm.getValue());
            } else if (metricName.equals("tcp_out_segs")) {
                Assert.assertEquals(Long.valueOf(1166716939L), (Long) gm.getValue());
            } else {
                Assert.fail();
            }
        }
    }

    @Test
    public void testUserQueryMetrics() {
        MetricRepo.USER_COUNTER_QUERY_ALL.getOrAdd("test_user").increase(1L);
        MetricRepo.USER_COUNTER_QUERY_ERR.getOrAdd("test_user").increase(1L);
        MetricRepo.USER_HISTO_QUERY_LATENCY.getOrAdd("test_user").update(10L);
        MetricVisitor visitor = new PrometheusMetricVisitor();
        MetricRepo.DORIS_METRIC_REGISTER.accept(visitor);
        SortedMap<String, Histogram> histograms = MetricRepo.METRIC_REGISTER.getHistograms();
        for (Map.Entry<String, Histogram> entry : histograms.entrySet()) {
            visitor.visitHistogram(MetricVisitor.FE_PREFIX, entry.getKey(), entry.getValue());
        }
        String metricResult = visitor.finish();
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_query_total counter"));
        Assert.assertTrue(metricResult.contains("doris_fe_query_total{user=\"test_user\"} 1"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_query_err counter"));
        Assert.assertTrue(metricResult.contains("doris_fe_query_err{user=\"test_user\"} 1"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_query_latency_ms summary"));
        Assert.assertTrue(metricResult.contains("doris_fe_query_latency_ms{quantile=\"0.999\"} 0.0"));
        Assert.assertTrue(metricResult.contains("doris_fe_query_latency_ms{quantile=\"0.999\",user=\"test_user\"} 10.0"));

    }
}
