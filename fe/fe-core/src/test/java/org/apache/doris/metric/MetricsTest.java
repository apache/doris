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
import org.apache.doris.common.util.JsonUtil;
import org.apache.doris.metric.Metric.MetricUnit;
import org.apache.doris.monitor.jvm.JvmService;
import org.apache.doris.monitor.jvm.JvmStats;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Slf4j
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
        MetricRepo.USER_HISTO_QUERY_LATENCY.getOrAdd("qing.lu@lbk.one").update(20L);
        MetricVisitor visitor = new PrometheusMetricVisitor();
        MetricRepo.DORIS_METRIC_REGISTER.accept(visitor);
        MetricRepo.visitHistograms(visitor);
        String metricResult = visitor.finish();
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_query_total counter"));
        Assert.assertTrue(metricResult.contains("doris_fe_query_total{user=\"test_user\"} 1"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_query_err counter"));
        Assert.assertTrue(metricResult.contains("doris_fe_query_err{user=\"test_user\"} 1"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_query_latency_ms summary"));
        Assert.assertTrue(metricResult.contains("doris_fe_query_latency_ms{quantile=\"0.999\"} 0.0"));
        Assert.assertTrue(metricResult.contains("doris_fe_query_latency_ms{quantile=\"0.999\",user=\"test_user\"} 10.0"));
        Assert.assertTrue(metricResult.contains(
                "doris_fe_query_latency_ms{quantile=\"0.999\",user=\"qing.lu@lbk.one\"} 20.0"));
        Assert.assertFalse(metricResult.contains("doris_fe_query_latency_ms_lu@lbk_one"));

    }

    @Test
    public void testPrometheusVisitorKeepsLabeledHistogramValuesOutOfMetricName() {
        HistogramMetric histogramMetric = new HistogramMetric("query.latency.ms",
                Lists.newArrayList(new MetricLabel("user", "thomas.liu@developertools.com")));
        histogramMetric.update(30L);
        MetricVisitor prometheusVisitor = new PrometheusMetricVisitor();
        prometheusVisitor.visitHistogram(MetricVisitor.FE_PREFIX, histogramMetric.getName(),
                histogramMetric.getHistogram(), histogramMetric.getLabels());
        String prometheusResult = prometheusVisitor.finish();
        Assert.assertTrue(prometheusResult.contains(
                "doris_fe_query_latency_ms{quantile=\"0.999\",user=\"thomas.liu@developertools.com\"} 30.0"));
        Assert.assertFalse(prometheusResult.contains("doris_fe_query_latency_ms_liu@developertools_com"));
        Assert.assertFalse(prometheusResult.contains("user=\"thomas\""));
    }

    @Test
    public void testJsonVisitorKeepsLabeledHistogramValuesOutOfMetricName() {
        HistogramMetric histogramMetric = new HistogramMetric("query.latency.ms",
                Lists.newArrayList(new MetricLabel("user", "qing.lu@lbk.one")));
        histogramMetric.update(20L);
        MetricVisitor jsonVisitor = new JsonMetricVisitor();
        jsonVisitor.visitHistogram(MetricVisitor.FE_PREFIX, histogramMetric.getName(),
                histogramMetric.getHistogram(), histogramMetric.getLabels());
        String jsonResult = jsonVisitor.finish();
        Assert.assertTrue(jsonResult.contains("\"metric\":\"doris_fe_query_latency_ms\""));
        Assert.assertTrue(jsonResult.contains("\"user\":\"qing.lu@lbk.one\""));
        Assert.assertFalse(jsonResult.contains("\"metric\":\"doris_fe_query_latency_ms_lu@lbk_one\""));
    }

    @Test
    public void testHistogramMetricRegistryWithSpecialCharacters() {
        DorisMetricRegistry registry = new DorisMetricRegistry();
        AutoMappedMetric<HistogramMetric> clusterHisto = new AutoMappedMetric<>(key -> {
            String[] values = key.split(CloudMetrics.CLOUD_CLUSTER_DELIMITER, 2);
            return new HistogramMetric("query.latency.ms", Lists.newArrayList(
                    new MetricLabel("cluster_id", values[0]), new MetricLabel("cluster_name", values[1])));
        });
        AutoMappedMetric<HistogramMetric> metaHisto = new AutoMappedMetric<>(methodName ->
                new HistogramMetric("meta_service.rpc.latency.ms",
                        Lists.newArrayList(new MetricLabel("method", methodName))));
        AutoMappedMetric<HistogramMetric> disabledHisto = new AutoMappedMetric<>(name ->
                new HistogramMetric("disabled.latency.ms",
                        Lists.newArrayList(new MetricLabel("name", name))));
        AutoMappedMetric<HistogramMetric> staleHisto = new AutoMappedMetric<>(name ->
                new HistogramMetric("stale.latency.ms",
                        Lists.newArrayList(new MetricLabel("name", name))));

        registry.addHistogramMetrics("cluster_query_latency", staleHisto);
        registry.addHistogramMetrics("cluster_query_latency", clusterHisto);
        registry.addHistogramMetrics("meta_service_rpc_latency", metaHisto);
        registry.addHistogramMetrics("disabled_latency", disabledHisto, () -> false);

        String clusterKey = "cluster.id-1" + CloudMetrics.CLOUD_CLUSTER_DELIMITER + "cluster.name@prod";
        staleHisto.getOrAdd("stale.name").update(30L);
        clusterHisto.getOrAdd(clusterKey).update(40L);
        metaHisto.getOrAdd("get.Instance").update(50L);
        disabledHisto.getOrAdd("disabled.name").update(60L);

        MetricVisitor prometheusVisitor = new PrometheusMetricVisitor();
        registry.acceptHistograms(prometheusVisitor);
        String prometheusResult = prometheusVisitor.finish();
        Assert.assertTrue(prometheusResult.contains(
                "doris_fe_query_latency_ms{quantile=\"0.999\",cluster_id=\"cluster.id-1\","
                        + "cluster_name=\"cluster.name@prod\"} 40.0"));
        Assert.assertTrue(prometheusResult.contains(
                "doris_fe_meta_service_rpc_latency_ms{quantile=\"0.999\",method=\"get.Instance\"} 50.0"));
        Assert.assertFalse(prometheusResult.contains("doris_fe_query_latency_ms_id-1_cluster"));
        Assert.assertFalse(prometheusResult.contains("doris_fe_meta_service_rpc_latency_ms_Instance"));
        Assert.assertFalse(prometheusResult.contains("doris_fe_stale_latency_ms"));
        Assert.assertFalse(prometheusResult.contains("doris_fe_disabled_latency_ms"));
    }

    @Test
    public void testGc() {
        PrometheusMetricVisitor visitor = new PrometheusMetricVisitor();
        JvmService jvmService = new JvmService();
        JvmStats jvmStats = jvmService.stats();
        visitor.visitJvm(jvmStats);
        String metric = MetricRepo.getMetric(visitor);
        List<GarbageCollectorMXBean> gcMxBeans = ManagementFactory.getGarbageCollectorMXBeans();
        String finalMetricPrometheus = metric;
        gcMxBeans.forEach(gcMxBean -> {
            String name = gcMxBean.getName();
            Assert.assertTrue(finalMetricPrometheus.contains("jvm_gc{name=\"" + name + " Count\", type=\"count\"} "));
            Assert.assertTrue(finalMetricPrometheus.contains("jvm_gc{name=\"" + name + " Time\", type=\"time\"} "));
        });

        JsonMetricVisitor jsonMetricVisitor = new JsonMetricVisitor();
        jsonMetricVisitor.visitJvm(jvmStats);
        metric = MetricRepo.getMetric(jsonMetricVisitor);
        String finalMetricJson = metric;
        AtomicInteger size = new AtomicInteger(JsonUtil.parseArray(finalMetricJson).size());
        gcMxBeans.forEach(gcMxBean -> JsonUtil.parseArray(finalMetricJson).forEach(json -> {
            ObjectNode jsonObject = JsonUtil.parseObject(json.toString());
            String name = gcMxBean.getName();
            if (jsonObject.findValue("tags").findValue("metric").asText().equals("jvm_gc")
                    && jsonObject.findValue("tags").findValue("name").asText().contains(name + " Count")) {
                size.getAndDecrement();
                Assert.assertTrue(jsonObject.findValue("tags").findValue("name").asText().contains(name + " Count")
                        || jsonObject.findValue("tags").findValue("name").asText().contains(name + " Time"));
            }

        }));
        Assert.assertTrue(size.get() < JsonUtil.parseArray(finalMetricJson).size());

    }

    @Test
    public void testCatalogAndDatabaseMetrics() {
        List<Metric> catalogMetrics = MetricRepo.getMetricsByName("catalog_num");
        Assert.assertEquals(1, catalogMetrics.size());
        GaugeMetric<Integer> catalogMetric = (GaugeMetric<Integer>) catalogMetrics.get(0);
        Assert.assertEquals("catalog_num", catalogMetric.getName());
        Assert.assertEquals(MetricUnit.NOUNIT, catalogMetric.getUnit());
        Assert.assertEquals("total catalog num", catalogMetric.getDescription());

        List<Metric> dbMetrics = MetricRepo.getMetricsByName("internal_database_num");
        Assert.assertEquals(1, dbMetrics.size());
        GaugeMetric<Integer> dbMetric = (GaugeMetric<Integer>) dbMetrics.get(0);
        Assert.assertEquals("internal_database_num", dbMetric.getName());
        Assert.assertEquals(MetricUnit.NOUNIT, dbMetric.getUnit());
        Assert.assertEquals("total internal database num", dbMetric.getDescription());

        List<Metric> tableMetrics = MetricRepo.getMetricsByName("internal_table_num");
        Assert.assertEquals(1, tableMetrics.size());
        GaugeMetric<Integer> tableMetric = (GaugeMetric<Integer>) tableMetrics.get(0);
        Assert.assertEquals("internal_table_num", tableMetric.getName());
        Assert.assertEquals(MetricUnit.NOUNIT, tableMetric.getUnit());
        Assert.assertEquals("total internal table num", tableMetric.getDescription());

        // Test metrics in Prometheus format
        MetricVisitor visitor = new PrometheusMetricVisitor();
        MetricRepo.DORIS_METRIC_REGISTER.accept(visitor);
        String metricResult = visitor.finish();

        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_catalog_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_internal_database_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_internal_table_num gauge"));

        // Test metrics in JSON format
        JsonMetricVisitor jsonVisitor = new JsonMetricVisitor();
        MetricRepo.DORIS_METRIC_REGISTER.accept(jsonVisitor);
        String jsonResult = jsonVisitor.finish();

        Assert.assertTrue(jsonResult.contains("\"metric\":\"doris_fe_catalog_num\""));
        Assert.assertTrue(jsonResult.contains("\"metric\":\"doris_fe_internal_database_num\""));
        Assert.assertTrue(jsonResult.contains("\"metric\":\"doris_fe_internal_table_num\""));
    }

    @Test
    public void testMTMVMetrics() {
        // Test metrics in Prometheus format
        MetricVisitor visitor = new PrometheusMetricVisitor();
        // doris metrics and system metrics.
        MetricRepo.DORIS_METRIC_REGISTER.accept(visitor);
        // histogram
        MetricRepo.visitHistograms(visitor);
        String metricResult = visitor.finish();

        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_async_materialized_view_task_failed_num counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_async_materialized_view_task_success_num counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_async_materialized_view_task_pending_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_async_materialized_view_task_skip_num counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_async_materialized_view_task_running_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_async_materialized_view_num gauge"));
        Assert.assertTrue(
                metricResult.contains("# TYPE doris_fe_async_materialized_view_task_duration_ms summary"));
    }

    @Test
    public void testStatisticsMetrics() {
        // Test metrics in Prometheus format
        MetricVisitor visitor = new PrometheusMetricVisitor();
        // doris metrics and system metrics.
        MetricRepo.DORIS_METRIC_REGISTER.accept(visitor);
        // histogram
        MetricRepo.visitHistograms(visitor);
        String metricResult = visitor.finish();

        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_succeed_analyze_job counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_failed_analyze_job counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_succeed_analyze_task counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_failed_analyze_task counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_invalid_stats counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_unhealthy_table_rate gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_unhealthy_column_rate gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_unhealthy_table_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_unhealthy_column_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_not_analyzed_table_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_empty_table_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_high_priority_queue_length gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_mid_priority_queue_length gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_low_priority_queue_length gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_statistics_very_low_priority_queue_length gauge"));
    }

    @Test
    public void testSqlCacheMetrics() {
        // Test metrics in Prometheus format
        MetricVisitor visitor = new PrometheusMetricVisitor();
        // doris metrics and system metrics.
        MetricRepo.DORIS_METRIC_REGISTER.accept(visitor);
        // histogram
        MetricRepo.visitHistograms(visitor);
        String metricResult = visitor.finish();

        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_sql_cache_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_sql_cache_added counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_sql_cache_hit counter"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_sql_cache_total_search_times counter"));
    }

    @Test
    public void testPlanMetrics() {
        // Test metrics in Prometheus format
        MetricVisitor visitor = new PrometheusMetricVisitor();
        // doris metrics and system metrics.
        MetricRepo.DORIS_METRIC_REGISTER.accept(visitor);
        // histogram
        MetricRepo.visitHistograms(visitor);
        String metricResult = visitor.finish();

        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_num gauge"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_parse_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_analyze_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_rewrite_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_fold_const_by_be_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_optimize_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_translate_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_init_scan_node_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_finalize_scan_node_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_create_scan_range_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_distribute_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_external_catalog_meta_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_external_tvf_init_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_lock_tables_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_partition_prune_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_cloud_meta_duration_ms summary"));
        Assert.assertTrue(metricResult.contains("# TYPE doris_fe_plan_materialized_view_rewrite_duration_ms summary"));
    }
}
