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

#include "jni_metrics.h"

#include <memory>
#include <utility>

#include "common/status.h"
#include "util/jni-util.h"
#include "util/metrics.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(jdbc_scan_connection_percent, MetricUnit::PERCENT);
DEFINE_GAUGE_METRIC_PROTOTYPE_2ARG(max_jdbc_scan_connection_percent, MetricUnit::PERCENT);
struct JdbcConnectionMetrics {
    JdbcConnectionMetrics(std::shared_ptr<MetricEntity> entity) : entity(entity) {
        INT_GAUGE_METRIC_REGISTER(entity, jdbc_scan_connection_percent);
    }
    void upate(int value) { jdbc_scan_connection_percent->set_value(value); }
    std::shared_ptr<MetricEntity> entity;
    IntGauge* jdbc_scan_connection_percent;
};

const char* JniMetrics::_s_hook_name = "jni_metrics";

JniMetrics::JniMetrics(MetricRegistry* registry) {
    DCHECK(registry != nullptr);
    _registry = registry;
    _server_entity = _registry->register_entity("server");
    DCHECK(_server_entity != nullptr);
    Status st = _init();
    if (!st.ok()) {
        LOG(WARNING) << "init jni metric failed. " << st.to_string();
    }
}

Status JniMetrics::_init() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    RETURN_IF_ERROR(JniUtil::get_jni_scanner_class(env, "org/apache/doris/jdbc/JdbcDataSource",
                                                   &_jdbc_data_source_clz));
    JNI_CALL_METHOD_CHECK_EXCEPTION(
            , _get_connection_percent_id, env,
            GetStaticMethodID(_jdbc_data_source_clz, "getConnectionPercent", "()Ljava/util/Map;"));
    _server_entity->register_hook(_s_hook_name,
                                  std::bind(&JniMetrics::update_jdbc_connection_metrics, this));
    INT_GAUGE_METRIC_REGISTER(_server_entity, max_jdbc_scan_connection_percent);
    max_jdbc_scan_connection_percent->set_value(0);
    _is_init = true;
    LOG(INFO) << "jni metrics inited successfully";
    return Status::OK();
}

Status JniMetrics::update_jdbc_connection_metrics() {
    JNIEnv* env = nullptr;
    RETURN_IF_ERROR(JniUtil::GetJNIEnv(&env));
    JNI_CALL_METHOD_CHECK_EXCEPTION_DELETE_REF(
            jobject, metrics, env,
            CallStaticObjectMethod(_jdbc_data_source_clz, _get_connection_percent_id));
    std::map<std::string, std::string> result = JniUtil::convert_to_cpp_map(env, metrics);
    for (auto item : result) {
        std::string catalog_id = item.first;
        int percent = std::stoi(item.second);
        auto iter = _jdbc_connection_metrics.find(catalog_id);
        if (iter == _jdbc_connection_metrics.end()) {
            auto entity =
                    _registry->register_entity_unlocked(catalog_id, {{"catalog", catalog_id}});
            _jdbc_connection_metrics.emplace(catalog_id,
                                             std::make_shared<JdbcConnectionMetrics>(entity));
        }
        _jdbc_connection_metrics[catalog_id]->upate(percent);
        if (percent > max_jdbc_scan_connection_percent->value()) {
            max_jdbc_scan_connection_percent->set_value(percent);
        }
    }
    // remove unused catalog
    for (auto& item : _jdbc_connection_metrics) {
        auto iter = result.find(item.first);
        if (iter == result.end()) {
            _jdbc_connection_metrics.erase(item.first);
            _registry->deregister_entity_unlocked(item.second->entity);
            LOG(INFO) << "catalog id : " << item.first << " unused, removed.";
        }
    }
    return Status::OK();
}

JniMetrics::~JniMetrics() {
    JNIEnv* env = nullptr;
    Status st = JniUtil::GetJNIEnv(&env);
    if (_is_init && st.ok()) {
        env->DeleteGlobalRef(_jdbc_data_source_clz);
    } else {
        LOG(WARNING) << "get jni env failed. " << st.to_string();
    }
    _server_entity->deregister_hook(_s_hook_name);
    for (auto& it : _jdbc_connection_metrics) {
        _registry->deregister_entity(it.second->entity);
    }
}

} // namespace doris
