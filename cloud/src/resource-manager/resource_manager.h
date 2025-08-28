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

#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

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
     * @param replace_if_existing_empty_target_cluster, find cluster.cluster_name is a empty cluster(no node), drop it
     * @filter filter condition
     * @return empty string for success, otherwise failure reason returned
     */
    virtual std::string update_cluster(
            const std::string& instance_id, const ClusterInfo& cluster,
            std::function<bool(const ClusterPB&)> filter,
            std::function<std::string(ClusterPB&, std::set<std::string>& cluster_names)> action,
            bool replace_if_existing_empty_target_cluster = false);

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
    /**
     * Modifies the nodes associated with a given instance.
     * This function allows adding and removing nodes from the instance.
     *
     * @param instance_id The ID of the instance to modify nodes for.
     * @param to_add A vector of NodeInfo structures representing nodes to be added.
     * @param to_del A vector of NodeInfo structures representing nodes to be removed.
     * @return An error message if the operation fails, or an empty string for success.
     */
    virtual std::string modify_nodes(const std::string& instance_id,
                                     const std::vector<NodeInfo>& to_add,
                                     const std::vector<NodeInfo>& to_del);

    /**
     * Checks the validity of the parameters for a cluster.
     * This function verifies if the provided cluster parameters meet the required conditions.
     *
     * @param cluster The ClusterPB structure containing the cluster parameters to validate.
     * @param err Output parameter to store any error message if validation fails.
     * @param check_master_num Flag indicating whether to check the number of master nodes.
     * @param check_cluster_name Flag indicating whether to check the cluster name is empty, just add_cluster need.
     * @return True if the parameters are valid, false otherwise.
     */
    bool check_cluster_params_valid(const ClusterPB& cluster, std::string* err,
                                    bool check_master_num, bool check_cluster_name);

    /**
     * Check cloud_unique_id is degraded format, and get instance_id from cloud_unique_id
     * degraded format : "${version}:${instance_id}:${unique_id}"
     * @param degraded cloud_unique_id
     *
     * @return a <is_degraded_format, instance_id> pair, if is_degraded_format == true , instance_id, if is_degraded_format == false, instance_id=""
     */
    static std::pair<bool, std::string> get_instance_id_by_cloud_unique_id(
            const std::string& cloud_unique_id);

    /**
     * check instance_id is a valid instance, check by get fdb kv 
     *
     * @param instance_id
     *
     * @return true, instance_id in fdb kv
     */
    bool is_instance_id_registered(const std::string& instance_id);

    /**
     * Refreshes the cache of given instance. This process removes the instance in cache
     * and then replaces it with persisted instance state read from underlying KV storage.
     *
     * @param instance_id instance to manipulate
     * @return a pair of code and msg
     */
    virtual std::pair<MetaServiceCode, std::string> refresh_instance(
            const std::string& instance_id);

    /**
     * Refreshes the cache of given instance from provided InstanceInfoPB. This process
     * removes the instance in cache and then replaces it with provided instance state.
     *
     * @param instance_id instance to manipulate
     * @param instance the instance info to refresh from
     */
    virtual void refresh_instance(const std::string& instance_id, const InstanceInfoPB& instance);

    virtual bool is_version_read_enabled(std::string_view instance_id) const;

    virtual bool is_version_write_enabled(std::string_view instance_id) const;

private:
    void add_cluster_to_index_no_lock(const std::string& instance_id, const ClusterPB& cluster);

    MultiVersionStatus get_instance_multi_version_status(std::string_view instance_id) const;

    mutable std::shared_mutex mtx_;
    // cloud_unique_id -> NodeInfo
    std::multimap<std::string, NodeInfo> node_info_;

    // instance_id -> MultiVersionStatus
    std::unordered_map<std::string, MultiVersionStatus> instance_multi_version_status_;

    std::shared_ptr<TxnKv> txn_kv_;
};

} // namespace doris::cloud
