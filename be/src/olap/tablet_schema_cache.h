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

#pragma once

#include <gen_cpp/olap_file.pb.h>

#include <memory>
#include <mutex>
#include <unordered_map>

#include "olap/tablet_schema.h"
#include "runtime/exec_env.h"
#include "util/doris_metrics.h"

namespace doris {

class TabletSchemaCache {
public:
    ~TabletSchemaCache() = default;

    static TabletSchemaCache* create_global_schema_cache() {
        TabletSchemaCache* res = new TabletSchemaCache();
        return res;
    }

    static TabletSchemaCache* instance() {
        return ExecEnv::GetInstance()->get_tablet_schema_cache();
    }

    TabletSchemaSPtr insert(const std::string& key);

    void start();

    void stop();

private:
    /**
     * @brief recycle when TabletSchemaSPtr use_count equals 1.
     */
    void _recycle();

private:
    std::mutex _mtx;
    std::unordered_map<std::string, TabletSchemaSPtr> _cache;
    std::atomic_bool _should_stop = {false};
    std::atomic_bool _is_stopped = {false};
};

} // namespace doris
