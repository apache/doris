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

#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"
#include "resource-manager/resource_manager.h"

using namespace doris::cloud;

static std::string mock_instance = "test_instance";
static std::string mock_cluster_name = "test_cluster";
static std::string mock_cluster_id = "test_cluster_id";

class MockResourceManager : public ResourceManager {
public:
    MockResourceManager(std::shared_ptr<TxnKv> txn_kv) : ResourceManager(txn_kv) {};
    ~MockResourceManager() override = default;

    int init() override { return 0; }

    std::string get_node(const std::string& cloud_unique_id,
                         std::vector<NodeInfo>* nodes) override {
        NodeInfo i {Role::COMPUTE_NODE, mock_instance, mock_cluster_name, mock_cluster_id};
        nodes->push_back(i);
        return "";
    }

    std::pair<MetaServiceCode, std::string> add_cluster(const std::string& instance_id,
                                                        const ClusterInfo& cluster) override {
        return std::make_pair(MetaServiceCode::OK, "");
    }

    std::pair<MetaServiceCode, std::string> drop_cluster(const std::string& instance_id,
                                                         const ClusterInfo& cluster) override {
        return std::make_pair(MetaServiceCode::OK, "");
    }

    std::pair<MetaServiceCode, std::string> refresh_instance(
            const std::string& instance_id) override {
        return std::make_pair(MetaServiceCode::OK, "");
    }

    std::string update_cluster(
            const std::string& instance_id, const ClusterInfo& cluster,
            std::function<bool(const ClusterPB&)> filter,
            std::function<std::string(ClusterPB&, std::set<std::string>& cluster_names)> action)
            override {
        return "";
    }

    std::pair<TxnErrorCode, std::string> get_instance(std::shared_ptr<Transaction> txn,
                                                      const std::string& instance_id,
                                                      InstanceInfoPB* inst_pb) override {
        return {TxnErrorCode::TXN_KEY_NOT_FOUND, ""};
    }

    std::string modify_nodes(const std::string& instance_id, const std::vector<NodeInfo>& to_add,
                             const std::vector<NodeInfo>& to_del) override {
        return "";
    }
};