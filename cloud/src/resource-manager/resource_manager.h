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

#include <gen_cpp/cloud.pb.h>

#include <map>
#include <shared_mutex>
#include <string>

#include "meta-service/txn_kv.h"
#include "meta-service/txn_kv_error.h"

namespace doris::cloud {

enum class Role : int {
    UNDEFINED,
    SQL_SERVER,
    COMPUTE_NODE,
};

struct NodeInfo {
    Role role;
    std::string instance_id;
    std::string cluster_name;
    std::string cluster_id;
    NodeInfoPB node_info;
};

struct ClusterInfo {
    ClusterPB cluster;
};

/**
 * This class manages resources referenced by cloud cloud.
 * It manage a in-memory
 */
class ResourceManager {
public:
    ResourceManager(std::shared_ptr<TxnKv> txn_kv) : txn_kv_(txn_kv) {};
    virtual ~ResourceManager() = default;
    /**
     * Loads all instance into memory and build an index
     *
     * @return 0 for success, non-zero for failure
     */
    virtual int init();

    /**
     * Gets nodes with given cloud unique id
     *
     * @param cloud_unique_id the cloud_unique_id attached to the node when it was added to the
     *                        instance
     * @param node output param
     * @return empty string for success, otherwise failure reason returned
     */
    virtual std::string get_node(const std::string& cloud_unique_id, std::vector<NodeInfo>* nodes);

    virtual std::pair<MetaServiceCode, std::string> add_cluster(const std::string& instance_id,
                                                                const ClusterInfo& cluster);

    /**
     * Drops a cluster
     *
     * @param cluster cluster to drop, only cluster name and cluster id are concered
     * @return empty string for success, otherwise failure reason returned
     */
    virtual std::pair<MetaServiceCode, std::string> drop_cluster(const std::string& instance_id,
                                                                 const ClusterInfo& cluster);

    /**
     * Update a cluster
     *
     * @param cluster cluster to update, only cluster name and cluster id are concered
     * @param action update operation code snippet
     * @filter filter condition
     * @return empty string for success, otherwise failure reason returned
     */
    virtual std::string update_cluster(
            const std::string& instance_id, const ClusterInfo& cluster,
            std::function<bool(const ClusterPB&)> filter,
            std::function<std::string(ClusterPB&, std::set<std::string>& cluster_names)> action);

    /**
     * Get instance from underlying storage with given transaction.
     *
     * @param txn if txn is not given, get with a new txn inside this function
     *
     * @return a <code, msg> pair, code == TXN_OK for success, otherwise error
     */
    virtual std::pair<TxnErrorCode, std::string> get_instance(std::shared_ptr<Transaction> txn,
                                                              const std::string& instance_id,
                                                              InstanceInfoPB* inst_pb);
    // return err msg
    virtual std::string modify_nodes(const std::string& instance_id,
                                     const std::vector<NodeInfo>& to_add,
                                     const std::vector<NodeInfo>& to_del);

    bool check_cluster_params_valid(const ClusterPB& cluster, std::string* err,
                                    bool check_master_num);

    /**
     * Refreshes the cache of given instance. This process removes the instance in cache
     * and then replaces it with persisted instance state read from underlying KV storage.
     *
     * @param instance_id instance to manipulate
     * @return a pair of code and msg
     */
    virtual std::pair<MetaServiceCode, std::string> refresh_instance(
            const std::string& instance_id);

private:
    void add_cluster_to_index(const std::string& instance_id, const ClusterPB& cluster);

    void remove_cluster_from_index(const std::string& instance_id, const ClusterPB& cluster);

    void update_cluster_to_index(const std::string& instance_id, const ClusterPB& original,
                                 const ClusterPB& now);

    void remove_cluster_from_index_no_lock(const std::string& instance_id,
                                           const ClusterPB& cluster);

    void add_cluster_to_index_no_lock(const std::string& instance_id, const ClusterPB& cluster);

private:
    std::shared_mutex mtx_;
    // cloud_unique_id -> NodeInfo
    std::multimap<std::string, NodeInfo> node_info_;

    std::shared_ptr<TxnKv> txn_kv_;
};

} // namespace doris::cloud
