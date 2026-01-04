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

#include <brpc/server.h>
#include <butil/endpoint.h>
#include <butil/strings/string_split.h>
#include <bvar/status.h>
#include <gen_cpp/cloud.pb.h>
#include <gen_cpp/olap_file.pb.h>

#include "recycler/checker.h"
#include "snapshot/snapshot_manager.h"

namespace doris::cloud {

int InstanceChecker::do_snapshots_check() {
    int ret = snapshot_manager_->check_snapshots(this);
    int success = 0;
    if (ret != 0) {
        if (ret == 1) {
            LOG(WARNING) << "failed to check snapshots"
                         << ", snapshot file lost or snapshot key leaked"
                         << ", instance_id=" << instance_id_;
        } else if (ret < 0) {
            LOG(WARNING) << "failed to check snapshots"
                         << ", instance_id=" << instance_id_;
        }
        success = 1;
    }
    ret = snapshot_manager_->inverted_check_snapshots(this);
    if (ret != 0) {
        if (ret == 1) {
            LOG(WARNING) << "failed to inverted check snapshots"
                         << ", snapshot key lost or snapshot file leaked"
                         << ", instance_id=" << instance_id_;
        } else if (ret < 0) {
            LOG(WARNING) << "failed to inverted check snapshots"
                         << ", instance_id=" << instance_id_;
        }
        success = 1;
    }
    return success;
}

int InstanceChecker::do_mvcc_meta_key_check() {
    int ret = snapshot_manager_->check_mvcc_meta_key(this);
    int success = 0;
    if (ret != 0) {
        if (ret == 1) {
            LOG(WARNING) << "failed to check mvcc meta key"
                         << ", segment file lost or key leaked"
                         << ", instance_id=" << instance_id_;
        } else if (ret < 0) {
            LOG(WARNING) << "failed to check mvcc meta key"
                         << ", instance_id=" << instance_id_;
        }
        success = 1;
    }
    ret = snapshot_manager_->inverted_check_mvcc_meta_key(this);
    if (ret != 0) {
        if (ret == 1) {
            LOG(WARNING) << "failed to inverted check mvcc meta key"
                         << ", segment key lost or segment file leaked"
                         << ", instance_id=" << instance_id_;
        } else if (ret < 0) {
            LOG(WARNING) << "failed to inverted check mvcc meta key"
                         << ", instance_id=" << instance_id_;
        }
        success = 1;
    }
    return success;
}

} // namespace doris::cloud