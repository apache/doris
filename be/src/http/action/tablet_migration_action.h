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

#include <string>

#include "common/status.h"
#include "gen_cpp/Status_types.h"
#include "http/http_handler.h"
#include "olap/data_dir.h"
#include "olap/tablet.h"
#include "util/threadpool.h"

namespace doris {

// Migrate a tablet from a disk to another.
class TabletMigrationAction : public HttpHandler {
public:
    TabletMigrationAction();
    void handle(HttpRequest* req) override;
    void _init_migration_action();
    Status _execute_tablet_migration(TabletSharedPtr tablet, DataDir* dest_store);
    Status _check_param(HttpRequest* req, int64_t& tablet_id, int32_t& schema_hash,
                        string& dest_disk, string& goal);
    Status _check_migrate_request(int64_t tablet_id, int32_t schema_hash, string dest_disk,
                                  TabletSharedPtr& tablet, DataDir** dest_store);

private:
    std::unique_ptr<ThreadPool> _migration_thread_pool;

    struct MigrationTask {
        MigrationTask(int64_t tablet_id, int32_t schema_hash)
                : _tablet_id(tablet_id), _schema_hash(schema_hash) {}

        MigrationTask(int64_t tablet_id, int32_t schema_hash, std::string dest_disk)
                : _tablet_id(tablet_id), _schema_hash(schema_hash), _dest_disk(dest_disk) {}

        bool operator<(const MigrationTask& right) const {
            if (_tablet_id != right._tablet_id) {
                return _tablet_id < right._tablet_id;
            } else if (_schema_hash != right._schema_hash) {
                return _schema_hash < right._schema_hash;
            } else {
                return false;
            }
        }

        std::string to_string() const {
            std::stringstream ss;
            ss << "MigrationTask: tablet_id=" << _tablet_id << ", schema_hash=" << _schema_hash
               << ", dest_disk=" << _dest_disk;
            return ss.str();
        }

        int64_t _tablet_id;
        int32_t _schema_hash;
        std::string _dest_disk;
    };

    std::mutex _migration_status_mutex;
    std::map<MigrationTask, std::string> _migration_tasks;
    std::deque<std::pair<MigrationTask, Status>> _finished_migration_tasks;
};
} // namespace doris
