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

#include "doris_bvar_metrics.h"
#include <sstream>

namespace doris {

#define INIT_INT64_BVAR_METRIC(name, type, unit, description, group_name, labels, core) \
    name = std::make_shared<BvarAdderMetric<int64_t>>(type, unit, #name, description, group_name, labels, core);

#define ENTITY_REGISTER_METRICS(name, type)                        \
auto name##_ptr = std::make_shared<BvarMetricEntity>(#name, type); \
entities_map_[#name].push_back(name##_ptr);                        \
name##_ptr->register_metric(#name, *name);                         \

// #define INIT_DOUBLE_BVAR_METRIC(name, type, unit, description, group_name, labels, core) \
//     name = std::make_shared<BvarAdderMetric<double>>(type, unit, #name, description, group_name, labels, core);

const std::string DorisBvarMetrics::s_registry_name_ = "doris_be";

DorisBvarMetrics::DorisBvarMetrics() {
    INIT_INT64_BVAR_METRIC(fragment_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "Total fragment requests received.", "", Labels(), false)
    INIT_INT64_BVAR_METRIC(fragment_request_duration_us, BvarMetricType::COUNTER, BvarMetricUnit::MICROSECONDS,
                           "", "", Labels(), false)
    INIT_INT64_BVAR_METRIC(query_scan_bytes, BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                           "", "", Labels(), false)   
    INIT_INT64_BVAR_METRIC(query_scan_rows, BvarMetricType::COUNTER, BvarMetricUnit::ROWS,
                           "", "", Labels(), false) 
    INIT_INT64_BVAR_METRIC(push_requests_success_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "push_requests_total", Labels({{"status", "SUCCESS"}}), false); 
    INIT_INT64_BVAR_METRIC(push_requests_fail_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "push_requests_total", Labels({{"status", "FAIL"}}), false); 
    INIT_INT64_BVAR_METRIC(push_request_duration_us, BvarMetricType::COUNTER, BvarMetricUnit::MICROSECONDS,
                           "", "", Labels(), false) 
    INIT_INT64_BVAR_METRIC(push_request_write_bytes, BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                           "", "", Labels(), false)
    INIT_INT64_BVAR_METRIC(push_request_write_rows, BvarMetricType::COUNTER, BvarMetricUnit::ROWS,
                           "", "", Labels(), false)
    INIT_INT64_BVAR_METRIC(create_tablet_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "create_tablet"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(create_tablet_requests_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "create_tablet"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(drop_tablet_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "drop_tablet"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(report_all_tablets_requests_skip, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "report_all_tablets"}, {"status", "skip"}}), false);
    INIT_INT64_BVAR_METRIC(schema_change_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total", Labels({{"type", "schema_change"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(schema_change_requests_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total", Labels({{"type", "schema_change"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(create_rollup_requests_total,BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "create_rollup"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(create_rollup_requests_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,  
                           "", "engine_requests_total", Labels({{"type", "create_rollup"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(storage_migrate_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total", Labels({{"type", "storage_migrate"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(storage_migrate_v2_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total", Labels({{"type", "storage_migrate_v2"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(storage_migrate_v2_requests_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,  "", "engine_requests_total", Labels({{"type", "storage_migrate_v2"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(delete_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "delete"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(delete_requests_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,  
                           "", "engine_requests_total", Labels({{"type", "delete"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(clone_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "clone"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(clone_requests_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "clone"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(alter_inverted_index_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "alter_inverted_index"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(alter_inverted_index_requests_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,  
                           "", "engine_requests_total", Labels({{"type", "alter_inverted_index"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(finish_task_requests_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,  
                           "", "engine_requests_total", Labels({{"type", "finish_task"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(finish_task_requests_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total", Labels({{"type", "finish_task"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(base_compaction_request_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total", Labels({{"type", "base_compaction"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(base_compaction_request_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total", Labels({{"type", "base_compaction"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(cumulative_compaction_request_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS, 
                           "", "engine_requests_total", Labels({{"type", "cumulative_compaction"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(cumulative_compaction_request_failed, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,  
                           "", "engine_requests_total", Labels({{"type", "cumulative_compaction"}, {"status", "failed"}}), false);
    INIT_INT64_BVAR_METRIC(base_compaction_deltas_total, BvarMetricType::COUNTER, BvarMetricUnit::ROWSETS,
                           "", "compaction_deltas_total", Labels({{"type", "base"}}), false);
    INIT_INT64_BVAR_METRIC(base_compaction_bytes_total, BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                           "", "compaction_bytes_total", Labels({{"type", "base"}}), false);
    INIT_INT64_BVAR_METRIC(cumulative_compaction_deltas_total, BvarMetricType::COUNTER, BvarMetricUnit::ROWSETS,
                           "", "compaction_deltas_total", Labels({{"type", "cumulative"}}), false);
    INIT_INT64_BVAR_METRIC(cumulative_compaction_bytes_total, BvarMetricType::COUNTER, BvarMetricUnit::BYTES, 
                           "", "compaction_bytes_total", Labels({{"type", "cumulative"}}), false);
    INIT_INT64_BVAR_METRIC(publish_task_request_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total", Labels({{"type", "publish"}, {"status", "total"}}), false);
    INIT_INT64_BVAR_METRIC(publish_task_failed_total, BvarMetricType::COUNTER, BvarMetricUnit::REQUESTS,
                           "", "engine_requests_total", Labels({{"type", "publish"}, {"status", "failed"}}), false);
    
    ENTITY_REGISTER_METRICS(fragment_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(fragment_request_duration_us, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(query_scan_bytes, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(query_scan_rows, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_requests_success_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_requests_fail_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_request_duration_us, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_request_write_bytes, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(push_request_write_rows, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(create_tablet_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(create_tablet_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(drop_tablet_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(report_all_tablets_requests_skip, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(schema_change_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(schema_change_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(create_rollup_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(create_rollup_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(storage_migrate_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(storage_migrate_v2_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(storage_migrate_v2_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(delete_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(delete_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(clone_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(clone_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(alter_inverted_index_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(alter_inverted_index_requests_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(finish_task_requests_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(finish_task_requests_failed, BvarMetricType::COUNTER)    
    ENTITY_REGISTER_METRICS(base_compaction_request_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(base_compaction_request_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(cumulative_compaction_request_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(cumulative_compaction_request_failed, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(base_compaction_deltas_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(base_compaction_bytes_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(cumulative_compaction_deltas_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(cumulative_compaction_bytes_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(publish_task_request_total, BvarMetricType::COUNTER)
    ENTITY_REGISTER_METRICS(publish_task_failed_total, BvarMetricType::COUNTER)
    
}

void DorisBvarMetrics::initialize(bool init_system_metrics, const std::set<std::string>& disk_devices,
                              const std::vector<std::string>& network_interfaces) {
    if (init_system_metrics) {
        system_metrics_ = std::make_unique<SystemBvarMetrics>();
    }
    auto file_create_total_ptr =
            std::make_shared<BvarMetricEntity>("file_create_total", BvarMetricType::COUNTER);
    entities_map_["file_create_total"].push_back(file_create_total_ptr);
    file_create_total_ptr->register_metric("file_create_total", g_adder_file_created_total);

    auto timeout_canceled_fragment_count =
            std::make_shared<BvarMetricEntity>("timeout_canceled", BvarMetricType::GAUGE);
    entities_map_["timeout_canceled"].push_back(timeout_canceled_fragment_count);
    timeout_canceled_fragment_count->register_metric("timeout_canceled_fragment_count",
                                         g_adder_timeout_canceled_fragment_count);

    auto test_ptr = std::make_shared<BvarMetricEntity>("test", BvarMetricType::COUNTER);
    entities_map_["test"].push_back(test_ptr);

    test_ptr->register_metric("fragment_request_duration", g_adder_fragment_request_duration_us);
    test_ptr->register_metric("query_scan_byte", g_adder_query_scan_bytes);
    test_ptr->register_metric("segment_read_total", g_adder_segment_read_total);
}

void DorisBvarMetrics::register_entity(BvarMetricEntity entity) {

}

std::string DorisBvarMetrics::to_prometheus() const{
    std::stringstream ss;
    for (auto& entities : entities_map_) {
        if (entities.second.empty()) {
            continue;
        }
        int count = 0;
        for (auto& entity : entities.second) {
            if (!count) {
                ss << "# TYPE " << s_registry_name_ << "_" << entity->get_name() << " " << entity->get_type() << "\n";
                count ++;
            }
            ss << entity->to_prometheus(s_registry_name_);
        }
    }
    if (system_metrics_) ss << system_metrics_->to_prometheus(s_registry_name_);
    return ss.str();
}

// timeout_canceled_fragment_count_entity
BvarAdderMetric<int64_t> g_adder_timeout_canceled_fragment_count(BvarMetricType::GAUGE,
                                                                 BvarMetricUnit::NOUNIT,
                                                                 "timeout_canceled_fragment_count",
                                                                 "", "", Labels());
BvarAdderMetric<int64_t> g_adder_file_created_total(BvarMetricType::COUNTER,
                                                    BvarMetricUnit::FILESYSTEM,
                                                    "file_created_total", "", "", Labels());
// test_entity
BvarAdderMetric<int64_t> g_adder_fragment_requests_total(BvarMetricType::COUNTER,
                                                         BvarMetricUnit::REQUESTS,
                                                         "fragment_requests_total",
                                                         "Total fragment requests received.", "",
                                                         Labels());
BvarAdderMetric<int64_t> g_adder_fragment_request_duration_us(BvarMetricType::COUNTER,
                                                              BvarMetricUnit::REQUESTS,
                                                              "fragment_request_duration_us", "",
                                                              "", Labels());
BvarAdderMetric<int64_t> g_adder_query_scan_bytes(BvarMetricType::COUNTER, BvarMetricUnit::BYTES,
                                                  "query_scan_bytes", "", "", Labels());
BvarAdderMetric<int64_t> g_adder_segment_read_total(BvarMetricType::COUNTER,
                                                    BvarMetricUnit::OPERATIONS,
                                                    "segment_read_total",
                                                    "(segment_v2) toal number of segments read",
                                                    "segment_read",
                                                    Labels({{"type", "segment_read_total"}}));
} // namespace doris