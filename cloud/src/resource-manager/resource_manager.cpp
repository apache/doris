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

#include "resource_manager.h"

#include <gen_cpp/cloud.pb.h>

#include <mutex>
#include <regex>
#include <sstream>

#include "common/logging.h"
#include "common/string_util.h"
#include "common/util.h"
#include "cpp/sync_point.h"
#include "meta-service/meta_service_helper.h"
#include "meta-store/keys.h"
#include "meta-store/txn_kv_error.h"

namespace doris::cloud {

static std::atomic_int64_t seq = 0;

int ResourceManager::init() {
    // Scan all instances
    std::unique_ptr<Transaction> txn;
    TxnErrorCode err = txn_kv_->create_txn(&txn);
    if (err != TxnErrorCode::TXN_OK) {
        LOG(INFO) << "failed to init txn, err=" << err;
        return -1;
    }

    InstanceKeyInfo key0_info {""};
    InstanceKeyInfo key1_info {"\xff"}; // instance id are human readable strings
    std::string key0;
    std::string key1;
    instance_key(key0_info, &key0);
    instance_key(key1_info, &key1);

    std::unique_ptr<RangeGetIterator> it;

    int num_instances = 0;
    DORIS_CLOUD_DEFER_COPY(key0, key1) {
        LOG(INFO) << "get instances, num_instances=" << num_instances << " range=[" << hex(key0)
                  << "," << hex(key1) << "]";
    };

    //                     instance_id  instance
    std::vector<std::tuple<std::string, InstanceInfoPB>> instances;
    int limit = 10000;
    TEST_SYNC_POINT_CALLBACK("ResourceManager:init:limit", &limit);
    do {
        TxnErrorCode err = txn->get(key0, key1, &it, false, limit);
        TEST_SYNC_POINT_CALLBACK("ResourceManager:init:get_err", &err);
        if (err == TxnErrorCode::TXN_TOO_OLD) {
            LOG(WARNING) << "failed to get instance, err=txn too old, "
                         << " already read " << instances.size() << " instance, "
                         << " now fallback to non snapshot scan";
            err = txn_kv_->create_txn(&txn);
            if (err == TxnErrorCode::TXN_OK) {
                err = txn->get(key0, key1, &it);
            }
        }
        if (err != TxnErrorCode::TXN_OK) {
            LOG(WARNING) << "internal error, failed to get instance, err=" << err;
            return -1;
        }

        while (it->has_next()) {
            auto [k, v] = it->next();
            if (!it->has_next()) key0 = k;
            LOG(INFO) << "range get instance_key=" << hex(k);
            instances.emplace_back("", InstanceInfoPB {});
            auto& [instance_id, inst] = instances.back();

            if (!inst.ParseFromArray(v.data(), v.size())) {
                LOG(WARNING) << "malformed instance, unable to deserialize, key=" << hex(k);
                return -1;
            }
            // 0x01 "instance" ${instance_id} -> InstanceInfoPB
            k.remove_prefix(1); // Remove key space
            std::vector<std::tuple<std::variant<int64_t, std::string>, int, int>> out;
            int ret = decode_key(&k, &out);
            if (ret != 0) {
                LOG(WARNING) << "failed to decode key, ret=" << ret;
                return -2;
            }
            if (out.size() != 2) {
                LOG(WARNING) << "decoded size no match, expect 2, given=" << out.size();
            }
            instance_id = std::get<std::string>(std::get<0>(out[1]));

            LOG(INFO) << "get an instance, instance_id=" << instance_id
                      << " instance json=" << proto_to_json(inst);

            ++num_instances;
        }
        key0.push_back('\x00'); // Update to next smallest key for iteration
    } while (it->more());

    std::unique_lock l(mtx_);
    for (auto& [inst_id, inst] : instances) {
        for (auto& c : inst.clusters()) {
            add_cluster_to_index_no_lock(inst_id, c);
        }
        if (inst.has_multi_version_status()) {
            instance_multi_version_status_[inst_id] = inst.multi_version_status();
        }
    }

    return 0;
}

std::string ResourceManager::get_node(const std::string& cloud_unique_id,
                                      std::vector<NodeInfo>* nodes) {
    // FIXME(gavin): refresh the all instance if there is a miss?
    //               Or we can refresh all instances regularly to reduce
    //               read amplification.
    std::shared_lock l(mtx_);
    auto [s, e] = node_info_.equal_range(cloud_unique_id);
    if (s == node_info_.end() || s->first != cloud_unique_id) {
        LOG(INFO) << "cloud unique id not found cloud_unique_id=" << cloud_unique_id;
        return "cloud_unique_id not found";
    }
    nodes->reserve(nodes->size() + node_info_.count(cloud_unique_id));
    for (auto i = s; i != e; ++i) {
        nodes->emplace_back(i->second); // Just copy, it's cheap
    }
    return "";
}

bool ResourceManager::check_cluster_params_valid(const ClusterPB& cluster, std::string* err,
                                                 bool check_master_num, bool check_cluster_name) {
    // Check if the cluster has a type
    if (!cluster.has_type()) {
        *err = "cluster must have type arg";
        return false;
    }

    // Validate cluster name
    if (!validate_cluster_name(cluster, err, check_cluster_name)) {
        return false;
    }

    // Validate nodes
    if (!validate_nodes(cluster, err, check_master_num)) {
        return false;
    }

    // Validate virtual cluster specifics
    if (ClusterPB::VIRTUAL == cluster.type()) {
        if (!validate_virtual_cluster(cluster, err)) {
            return false;
        }
    }

    return true;
}

bool ResourceManager::validate_cluster_name(const ClusterPB& cluster, std::string* err,
                                            bool check_cluster_name) {
    if (check_cluster_name && (!cluster.has_cluster_name() || cluster.cluster_name() == "")) {
        *err = "not have cluster name";
        return false;
    }
    const char* cluster_pattern_str = "^[a-zA-Z][a-zA-Z0-9_]*$";
    std::regex txt_regex(cluster_pattern_str);
    if (config::enable_cluster_name_check && cluster.has_cluster_name() &&
        !std::regex_match(cluster.cluster_name(), txt_regex)) {
        *err = "cluster name not regex with ^[a-zA-Z][a-zA-Z0-9_]*$, please check it";
        return false;
    }
    return true;
}

bool ResourceManager::validate_nodes(const ClusterPB& cluster, std::string* err,
                                     bool check_master_num) {
    std::stringstream ss;
    bool no_err = true;
    int master_num = 0;
    int follower_num = 0;

    for (auto& n : cluster.nodes()) {
        // check here cloud_unique_id
        std::string cloud_unique_id = n.cloud_unique_id();
        auto [is_degrade_format, instance_id] = get_instance_id_by_cloud_unique_id(cloud_unique_id);
        if (config::enable_check_instance_id && is_degrade_format &&
            !is_instance_id_registered(instance_id)) {
            ss << "node=" << n.DebugString()
               << " cloud_unique_id use degrade format, but check instance failed";
            *err = ss.str();
            return false;
        }
        if (ClusterPB::SQL == cluster.type() && n.has_edit_log_port() && n.edit_log_port() &&
            n.has_node_type() &&
            (n.node_type() == NodeInfoPB_NodeType_FE_MASTER ||
             n.node_type() == NodeInfoPB_NodeType_FE_OBSERVER ||
             n.node_type() == NodeInfoPB_NodeType_FE_FOLLOWER)) {
            master_num += n.node_type() == NodeInfoPB_NodeType_FE_MASTER ? 1 : 0;
            follower_num += n.node_type() == NodeInfoPB_NodeType_FE_FOLLOWER ? 1 : 0;
            continue;
        } else if (ClusterPB::COMPUTE == cluster.type() && n.has_heartbeat_port() &&
                   n.heartbeat_port()) {
            continue;
        } else if (ClusterPB::VIRTUAL) {
            continue;
        }
        ss << "check cluster params failed, edit_log_port is required for frontends while "
              "heatbeat_port is required for banckends, node : "
           << proto_to_json(n);
        *err = ss.str();
        no_err = false;
        break;
    }

    if (check_master_num && ClusterPB::SQL == cluster.type()) {
        no_err = validate_master_follower_count(master_num, follower_num, err);
    }

    return no_err;
}

bool ResourceManager::validate_master_follower_count(int master_num, int follower_num,
                                                     std::string* err) {
    std::stringstream ss;
    if (master_num > 0 && follower_num > 0) {
        ss << "cluster is SQL type, and use multi follower mode, cant set master node, master "
              "count: "
           << master_num << " follower count: " << follower_num;
        *err = ss.str();
        return false;
    } else if (!follower_num && master_num != 1) {
        ss << "cluster is SQL type, must have only one master node, now master count: "
           << master_num;
        *err = ss.str();
        return false;
    }
    return true;
}

void set_default_failover_threshold(doris::cloud::ClusterPolicy& policy) {
    // if not set failover_failure_threshold in api, set here default value
    if (!policy.has_failover_failure_threshold()) {
        policy.set_failover_failure_threshold(3);
    }
    // if not set unhealthy_node_threshold_percent in api, set here default value
    if (!policy.has_unhealthy_node_threshold_percent()) {
        policy.set_unhealthy_node_threshold_percent(100);
    }
}

bool ResourceManager::validate_virtual_cluster(const ClusterPB& cluster, std::string* err) {
    if (cluster.nodes().size() || cluster.mysql_user_name().size() ||
        cluster.has_public_endpoint() || cluster.has_private_endpoint() ||
        cluster.has_cluster_status()) {
        *err = "Inconsistent virtual cluster args";
        LOG(WARNING) << *err;
        return false;
    }

    if ((cluster.cluster_names().size() && !cluster.has_cluster_policy()) ||
        (cluster.cluster_names().size() == 0 && cluster.has_cluster_policy())) {
        *err = "subcgs and policy must be Incoming at the same time or do not transmit at the same "
               "time";
        LOG(WARNING) << *err;
        return false;
    }
    // Validate cluster names
    if (cluster.cluster_names().size()) {
        // Currently, the number of sub clusters is limited to 2
        if (cluster.cluster_names().size() != 2) {
            *err = "Currently, just support two sub clusters";
            LOG(WARNING) << *err;
            return false;
        }

        for (const auto& sub_cluster : cluster.cluster_names()) {
            if (!std::regex_match(sub_cluster, std::regex("^[a-zA-Z][a-zA-Z0-9_]*$"))) {
                *err = "cluster name " + sub_cluster +
                       " does not match regex ^[a-zA-Z][a-zA-Z0-9_]*$";
                LOG(WARNING) << *err;
                return false;
            }
        }
    }

    // Validate cluster policy
    if (cluster.has_cluster_policy()) {
        const auto& policy = cluster.cluster_policy();
        if (!policy.has_type()) {
            *err = "plz set cluster policy type, use virtual cluster policy";
            LOG(WARNING) << *err;
            return false;
        }
        if (policy.type() != ClusterPolicy::ActiveStandby) {
            *err = "cluster policy type must be ActiveStandby";
            LOG(WARNING) << *err;
            return false;
        }
        if ((policy.active_cluster_name().empty() && !policy.standby_cluster_names().empty()) ||
            (!policy.active_cluster_name().empty() && policy.standby_cluster_names().empty())) {
            *err = "Inconsistent cluster policy: active_cluster_name must be set if "
                   "standby_cluster_names are present, and vice versa.";
            LOG(WARNING) << *err;
            return false;
        }
        if (policy.active_cluster_name().empty() ||
            std::find(cluster.cluster_names().begin(), cluster.cluster_names().end(),
                      policy.active_cluster_name()) == cluster.cluster_names().end()) {
            *err = "active_cluster_name must not be empty and must be in cluster_names";
            LOG(WARNING) << *err;
            return false;
        }
        for (const auto& standby_name : policy.standby_cluster_names()) {
            if (std::find(cluster.cluster_names().begin(), cluster.cluster_names().end(),
                          standby_name) == cluster.cluster_names().end()) {
                *err = "standby_cluster_name " + standby_name + " must be in cluster_names";
                LOG(WARNING) << *err;
                return false;
            }
            if (standby_name == policy.active_cluster_name()) {
                *err = "active_cluster_name is same of standby_cluster_name";
                LOG(WARNING) << *err;
                return false;
            }
        }

        set_default_failover_threshold(const_cast<std::decay_t<decltype(policy)>&>(policy));

        if (policy.failover_failure_threshold() <= 0 ||
            policy.unhealthy_node_threshold_percent() > INT64_MAX) {
            *err = "failover_failure_threshold must be greater than 0 and less than max(int64)";
            LOG(WARNING) << *err;
            return false;
        }
        if (policy.unhealthy_node_threshold_percent() <= 0 ||
            policy.unhealthy_node_threshold_percent() > 100) {
            *err = "unhealthy_node_threshold_percent must be greater than 0 and less than or equal "
                   "to 100";
            LOG(WARNING) << *err;
            return false;
        }
    }
    return true;
}

std::pair<bool, std::string> ResourceManager::get_instance_id_by_cloud_unique_id(
        const std::string& cloud_unique_id) {
    auto v = split(cloud_unique_id, ':');
    if (v.size() != 3) return {false, ""};
    // degraded format check it
    int version = std::atoi(v[0].c_str());
    if (version != 1) return {false, ""};
    return {true, v[1]};
}

bool ResourceManager::is_instance_id_registered(const std::string& instance_id) {
    // check kv
    auto [c0, m0] = get_instance(nullptr, instance_id, nullptr);
    { TEST_SYNC_POINT_CALLBACK("is_instance_id_registered", &c0); }
    if (c0 != TxnErrorCode::TXN_OK) {
        LOG(WARNING) << "failed to check instance instance_id=" << instance_id
                     << ", code=" << format_as(c0) << ", info=" + m0;
    }
    return c0 == TxnErrorCode::TXN_OK;
}

/**
 * Gets addr and port from NodeInfoPB
 * @param node
 * @return <addr, port>
 */
static std::pair<std::string, int32_t> get_node_endpoint_from_cluster(const ClusterPB::Type& type,
                                                                      const NodeInfoPB& node) {
    std::string addr = node.has_host() ? node.host() : (node.has_ip() ? node.ip() : "");
    int32_t port = (ClusterPB::SQL == type)
                           ? node.edit_log_port()
                           : (ClusterPB::COMPUTE == type ? node.heartbeat_port() : -1);
    return std::make_pair(addr, port);
}

/**
 * Gets nodes endpoint from InstanceInfoPB which are registered
 * @param instance
 * @return <fe_nodes, be_nodes>
 */
static std::pair<std::set<std::string>, std::set<std::string>> get_nodes_endpoint_registered(
        const InstanceInfoPB& instance) {
    std::set<std::string> instance_sql_node_endpoints;
    std::set<std::string> instance_compute_node_endpoints;
    for (auto& instance_cluster : instance.clusters()) {
        for (auto& node : instance_cluster.nodes()) {
            const auto& [addr, port] =
                    get_node_endpoint_from_cluster(instance_cluster.type(), node);
            if (ClusterPB::SQL == instance_cluster.type()) {
                instance_sql_node_endpoints.insert(addr + ":" + std::to_string(port));
            } else if (ClusterPB::COMPUTE == instance_cluster.type()) {
                instance_compute_node_endpoints.insert(addr + ":" + std::to_string(port));
            }
        }
    }
    return std::make_pair(instance_sql_node_endpoints, instance_compute_node_endpoints);
}

/**
 * When add_cluster or add_node, check its node has been registered
 * @param cluster type, for check port
 * @param node, which node to add
 * @param registered_fes, that fes has been registered in kv
 * @param registered_bes, that bes has been registered in kv
 * @return <error_code, err_msg>, if error_code == OK, check pass
 */
static std::pair<MetaServiceCode, std::string> check_node_has_been_registered(
        const ClusterPB::Type& type, const NodeInfoPB& node,
        std::set<std::string> fe_endpoints_registered,
        std::set<std::string> be_endpoints_registered) {
    const auto& [addr, port] = get_node_endpoint_from_cluster(type, node);
    std::stringstream ss;
    std::string msg;
    if (addr == "" || port == -1) {
        ss << "add node input args node invalid, cluster=" << proto_to_json(node);
        LOG(WARNING) << ss.str();
        msg = ss.str();
        return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    std::string node_endpoint = addr + ":" + std::to_string(port);

    if (type == ClusterPB::SQL) {
        if (fe_endpoints_registered.count(node_endpoint)) {
            ss << "sql node endpoint has been added, registered fe node=" << node_endpoint;
            LOG(WARNING) << ss.str();
            msg = ss.str();
            return std::make_pair(MetaServiceCode::ALREADY_EXISTED, msg);
        }
    } else if (type == ClusterPB::COMPUTE) {
        if (be_endpoints_registered.count(node_endpoint)) {
            ss << "compute node endpoint has been added, registered be node=" << node_endpoint;
            LOG(WARNING) << ss.str();
            msg = ss.str();
            return std::make_pair(MetaServiceCode::ALREADY_EXISTED, msg);
        }
    }
    return std::make_pair(MetaServiceCode::OK, "");
}

std::pair<MetaServiceCode, std::string> ResourceManager::add_cluster(const std::string& instance_id,
                                                                     const ClusterInfo& cluster) {
    std::string msg;
    std::stringstream ss;

    DORIS_CLOUD_DEFER {
        LOG(INFO) << "add_cluster err=" << msg;
    };

    // just check cluster_name not empty in add_cluster
    if (!check_cluster_params_valid(cluster.cluster, &msg, true, true)) {
        LOG(WARNING) << msg;
        return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    // FIXME(gavin): ensure atomicity of the entire process of adding cluster.
    //               Inserting a placeholer to node_info_ before persistence
    //               and updating it after persistence for the cloud_unique_id
    //               to add is a fairly fine solution.
    {
        std::shared_lock l(mtx_);
        // Check uniqueness of cloud_unique_id to add, cloud unique ids are not
        // shared between instances
        for (auto& i : cluster.cluster.nodes()) {
            // check cloud_unique_id in the same instance
            auto [start, end] = node_info_.equal_range(i.cloud_unique_id());
            if (start == node_info_.end() || start->first != i.cloud_unique_id()) continue;
            for (auto it = start; it != end; ++it) {
                if (it->second.instance_id != instance_id) {
                    // different instance, but has same cloud_unique_id
                    ss << "cloud_unique_id is already occupied by an instance,"
                       << " instance_id=" << it->second.instance_id
                       << " cluster_name=" << it->second.cluster_name
                       << " cluster_id=" << it->second.cluster_id
                       << " cloud_unique_id=" << it->first;
                    msg = ss.str();
                    LOG(INFO) << msg;
                    return std::make_pair(MetaServiceCode::ALREADY_EXISTED, msg);
                }
            }
        }
    }

    std::unique_ptr<Transaction> txn0;
    TxnErrorCode err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return std::make_pair(cast_as<ErrCategory::CREATE>(err), msg);
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != TxnErrorCode::TXN_OK) {
        msg = "failed to get instance, info " + m0;
        LOG(WARNING) << msg << " err=" << c0;
        return std::make_pair(cast_as<ErrCategory::READ>(c0), msg);
    }

    if (instance.status() == InstanceInfoPB::DELETED) {
        msg = "instance status has been set delete, plz check it";
        return std::make_pair(MetaServiceCode::CLUSTER_NOT_FOUND, msg);
    }

    auto& req_cluster = cluster.cluster;
    LOG(INFO) << "cluster to add json=" << proto_to_json(req_cluster);
    LOG(INFO) << "json=" << proto_to_json(instance);

    // Check id and name, they need to be unique
    // One cluster id per name, name is alias of cluster id
    std::vector<ClusterPB> clusters_in_instance;
    for (auto& i : instance.clusters()) {
        clusters_in_instance.emplace_back(i);
    }
    for (auto& i : clusters_in_instance) {
        if (i.cluster_id() == cluster.cluster.cluster_id()) {
            ss << "try to add a existing cluster id,"
               << " existing_cluster_id=" << i.cluster_id();
            msg = ss.str();
            return std::make_pair(MetaServiceCode::ALREADY_EXISTED, msg);
        }

        if (i.cluster_name() == req_cluster.cluster_name()) {
            ss << "try to add a existing cluster name,"
               << " existing_cluster_name=" << i.cluster_name();
            msg = ss.str();
            return std::make_pair(MetaServiceCode::ALREADY_EXISTED, msg);
        }
    }

    // modify cluster's node info
    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();

    const auto& [fe_endpoints_registered, be_endpoints_registered] =
            get_nodes_endpoint_registered(instance);

    for (auto& n : req_cluster.nodes()) {
        auto& node = const_cast<std::decay_t<decltype(n)>&>(n);
        node.set_ctime(time);
        node.set_mtime(time);
        // Check duplicated nodes, one node cannot deploy on multiple clusters
        // diff instance_cluster's nodes and req_cluster's nodes
        for (auto& n : req_cluster.nodes()) {
            const auto& [c1, m1] = check_node_has_been_registered(
                    req_cluster.type(), n, fe_endpoints_registered, be_endpoints_registered);
            if (c1 != MetaServiceCode::OK) {
                return std::make_pair(c1, m1);
            }
        }
    }
    // vitrual cluster
    if (cluster.cluster.type() == ClusterPB::VIRTUAL) {
        // if has cluster_names, check it's cluster has been added
        std::vector<std::string> check_clusters_vec(cluster.cluster.cluster_names().begin(),
                                                    cluster.cluster.cluster_names().end());
        auto validation_result = validate_sub_clusters(check_clusters_vec, clusters_in_instance);
        if (validation_result.first != MetaServiceCode::OK) {
            return validation_result;
        }
    }

    auto to_add_cluster = instance.add_clusters();
    to_add_cluster->CopyFrom(cluster.cluster);
    // create compute cluster, set it status normal as default value
    if (cluster.cluster.type() == ClusterPB::COMPUTE) {
        to_add_cluster->set_cluster_status(ClusterStatus::NORMAL);
    }
    to_add_cluster->set_ctime(time);
    to_add_cluster->set_mtime(time);
    LOG(INFO) << "instance " << instance_id << " has " << instance.clusters().size() << " clusters";

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_key=" << hex(key);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " err=" << err;
        return std::make_pair(cast_as<ErrCategory::COMMIT>(err), msg);
    }

    refresh_instance(instance_id, instance);

    return std::make_pair(MetaServiceCode::OK, "");
}

/**
 * The current implementation is to add fe clusters through HTTP API, 
 * such as follower nodes `ABC` in the cluster, and then immediately drop follower node `A`, while fe is not yet pulled up, 
 * which may result in the formation of a multi master fe cluster
 * This function provides a simple protection mechanism that does not allow dropping the fe node within 5 minutes after adding it through the API(add_cluster/add_node).
 * If you bypass this protection and do the behavior described above, god bless you.
 * @param node, which fe node to drop
 * @return true, can drop. false , within ctime 5 mins, can't drop
 */
static bool is_sql_node_exceeded_safe_drop_time(const NodeInfoPB& node) {
    int64_t ctime = node.ctime();
    // protect time 5mins
    int64_t exceed_time = 5 * 60;
    TEST_SYNC_POINT_CALLBACK("resource_manager::set_safe_drop_time", &exceed_time);
    exceed_time = ctime + exceed_time;
    auto now_time = std::chrono::system_clock::now();
    int64_t current_time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();
    return current_time > exceed_time;
}

std::pair<MetaServiceCode, std::string> ResourceManager::drop_cluster(
        const std::string& instance_id, const ClusterInfo& cluster) {
    std::stringstream ss;
    std::string msg;

    std::string cluster_id = cluster.cluster.has_cluster_id() ? cluster.cluster.cluster_id() : "";
    if (cluster_id.empty()) {
        ss << "missing cluster_id=" << cluster_id;
        msg = ss.str();
        LOG(INFO) << msg;
        return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
    }

    std::unique_ptr<Transaction> txn0;
    TxnErrorCode err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return std::make_pair(cast_as<ErrCategory::CREATE>(err), msg);
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 == TxnErrorCode::TXN_KEY_NOT_FOUND) {
        msg = m0;
        LOG(WARNING) << msg;
        return std::make_pair(MetaServiceCode::CLUSTER_NOT_FOUND, msg);
    }
    if (c0 != TxnErrorCode::TXN_OK) {
        msg = m0;
        LOG(WARNING) << msg;
        return std::make_pair(cast_as<ErrCategory::READ>(c0), msg);
    }

    if (instance.status() == InstanceInfoPB::DELETED) {
        msg = "instance status has been set delete, plz check it";
        return std::make_pair(MetaServiceCode::CLUSTER_NOT_FOUND, msg);
    }

    bool found = false;
    int idx = -1;
    std::string cache_err_help_msg =
            "Ms nodes memory cache may be inconsistent, pls check registry key may be contain "
            "127.0.0.1, and call get_instance api get instance info from fdb";
    ClusterPB to_del;
    // Check id, they need to be unique
    // One cluster id per name, name is alias of cluster id
    for (auto& i : instance.clusters()) {
        ++idx;
        if (i.cluster_id() == cluster.cluster.cluster_id()) {
            if (i.type() == ClusterPB::SQL) {
                for (auto& fe_node : i.nodes()) {
                    // check drop fe cluster
                    if (config::enable_check_fe_drop_in_safe_time &&
                        !is_sql_node_exceeded_safe_drop_time(fe_node)) {
                        ss << "drop fe cluster not in safe time, try later, cluster="
                           << i.DebugString();
                        msg = ss.str();
                        LOG(WARNING) << msg;
                        return std::make_pair(MetaServiceCode::CLUSTER_NOT_FOUND, msg);
                    }
                }
            }
            to_del.CopyFrom(i);
            LOG(INFO) << "found a cluster to drop,"
                      << " instance_id=" << instance_id << " cluster_id=" << i.cluster_id()
                      << " cluster_name=" << i.cluster_name() << " cluster=" << proto_to_json(i);
            found = true;
            break;
        }
    }

    if (!found) {
        ss << "failed to find cluster to drop,"
           << " instance_id=" << instance_id << " cluster_id=" << cluster.cluster.cluster_id()
           << " cluster_name=" << cluster.cluster.cluster_name()
           << " help Msg=" << cache_err_help_msg;
        msg = ss.str();
        LOG(WARNING) << msg;
        return std::make_pair(MetaServiceCode::CLUSTER_NOT_FOUND, msg);
    }

    // Checks whether the deleted cluster belongs to a virtual cluster.
    // If so, drop failed, need drop virtual cluster first
    for (auto& c : instance.clusters()) {
        if (ClusterPB::VIRTUAL == c.type() &&
            std::find(c.cluster_names().begin(), c.cluster_names().end(), to_del.cluster_name()) !=
                    c.cluster_names().end()) {
            ss << "failed to drop cluster, this cluster owned by virtual cluster="
               << c.cluster_name()
               << " if you want drop this cluster, please drop virtual cluster=" << c.cluster_name()
               << " firstly";
            msg = ss.str();
            LOG(WARNING) << msg;
            return std::make_pair(MetaServiceCode::ALREADY_EXISTED, msg);
        }
    }

    InstanceInfoPB new_instance(instance);
    new_instance.mutable_clusters()->DeleteSubrange(idx, 1); // Remove it

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = new_instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        LOG(WARNING) << msg;
        return std::make_pair(MetaServiceCode::PROTOBUF_SERIALIZE_ERR, msg);
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_key=" << hex(key);
    err = txn->commit();
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " err=" << err;
        return std::make_pair(cast_as<ErrCategory::COMMIT>(err), msg);
    }

    refresh_instance(instance_id, new_instance);

    return std::make_pair(MetaServiceCode::OK, "");
}

std::string ResourceManager::update_cluster(
        const std::string& instance_id, const ClusterInfo& cluster,
        std::function<bool(const ClusterPB&)> filter,
        std::function<std::string(ClusterPB&, std::vector<ClusterPB>& clusters_in_instance)> action,
        bool replace_if_existing_empty_target_cluster) {
    std::stringstream ss;
    std::string msg;

    std::string cluster_id = cluster.cluster.has_cluster_id() ? cluster.cluster.cluster_id() : "";
    std::string cluster_name =
            cluster.cluster.has_cluster_name() ? cluster.cluster.cluster_name() : "";
    if (cluster_id.empty()) {
        ss << "missing cluster_id=" << cluster_id;
        msg = ss.str();
        LOG(INFO) << msg;
        return msg;
    }

    std::unique_ptr<Transaction> txn0;
    TxnErrorCode err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return msg;
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != TxnErrorCode::TXN_OK) {
        msg = m0;
        return msg;
    }

    if (instance.status() == InstanceInfoPB::DELETED) {
        msg = "instance status has been set delete, plz check it";
        return msg;
    }

    std::vector<ClusterPB> clusters_in_instance;
    std::set<std::string> cluster_names;
    // collect cluster in instance pb for check
    for (auto& i : instance.clusters()) {
        cluster_names.emplace(i.cluster_name());
        clusters_in_instance.emplace_back(i);
    }

    bool found = false;
    int idx = -1;
    // Check id and name, they need to be unique
    // One cluster id per name, name is alias of cluster id
    for (auto& i : instance.clusters()) {
        ++idx;
        if (filter(i)) {
            LOG(INFO) << "found a cluster to update,"
                      << " instance_id=" << instance_id << " cluster_id=" << i.cluster_id()
                      << " cluster_name=" << i.cluster_name() << " cluster=" << proto_to_json(i);
            found = true;
            break;
        }
    }

    if (!found) {
        ss << "failed to find cluster to update,"
           << " instance_id=" << instance_id << " cluster_id=" << cluster.cluster.cluster_id()
           << " cluster_name=" << cluster.cluster.cluster_name();
        msg = ss.str();
        return msg;
    }

    auto& clusters = const_cast<std::decay_t<decltype(instance.clusters())>&>(instance.clusters());

    // check cluster_name is empty cluster, if empty and replace_if_existing_empty_target_cluster == true, drop it
    if (replace_if_existing_empty_target_cluster) {
        auto it = cluster_names.find(cluster_name);
        if (it != cluster_names.end()) {
            // found it, if it's an empty cluster, drop it from instance
            int idx = -1;
            for (auto& cluster : instance.clusters()) {
                idx++;
                if (cluster.cluster_name() == cluster_name) {
                    // Check if cluster is empty (has no nodes)
                    if (cluster.nodes_size() == 0) {
                        // Remove empty cluster from instance
                        auto& clusters = const_cast<std::decay_t<decltype(instance.clusters())>&>(
                                instance.clusters());
                        clusters.DeleteSubrange(idx, 1);
                        // Remove cluster name from set
                        cluster_names.erase(cluster_name);
                        LOG(INFO) << "remove empty cluster due to it is the target of a "
                                     "rename_cluster, cluster_name="
                                  << cluster_name;
                    }
                    break;
                }
            }
        }
    }

    // do update
    ClusterPB original = clusters[idx];
    msg = action(clusters[idx], clusters_in_instance);
    if (!msg.empty()) {
        return msg;
    }
    ClusterPB now = clusters[idx];
    auto now_time = std::chrono::system_clock::now();
    uint64_t time =
            std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch()).count();
    now.set_mtime(time);
    LOG(INFO) << "before update cluster original: " << proto_to_json(original)
              << " after update now: " << proto_to_json(now);

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        return msg;
    }

    txn->put(key, val);
    LOG(INFO) << "put instanace_key=" << hex(key);
    TxnErrorCode err_code = txn->commit();
    if (err_code != TxnErrorCode::TXN_OK) {
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " err=" << msg;
        return msg;
    }

    LOG(INFO) << "update cluster instance_id=" << instance_id
              << " instance json=" << proto_to_json(instance);

    refresh_instance(instance_id, instance);

    return msg;
}

void ResourceManager::add_cluster_to_index_no_lock(const std::string& instance_id,
                                                   const ClusterPB& c) {
    auto type = c.has_type() ? c.type() : -1;
    Role role = (type == ClusterPB::SQL
                         ? Role::SQL_SERVER
                         : (type == ClusterPB::COMPUTE ? Role::COMPUTE_NODE : Role::UNDEFINED));
    LOG(INFO) << "add cluster to index, instance_id=" << instance_id << " cluster_type=" << type
              << " cluster_name=" << c.cluster_name() << " cluster_id=" << c.cluster_id();

    for (auto& i : c.nodes()) {
        bool existed = node_info_.count(i.cloud_unique_id());
        NodeInfo n {.role = role,
                    .instance_id = instance_id,
                    .cluster_name = c.cluster_name(),
                    .cluster_id = c.cluster_id(),
                    .node_info = i};
        LOG(WARNING) << (existed ? "duplicated cloud_unique_id " : "")
                     << "instance_id=" << instance_id << " cloud_unique_id=" << i.cloud_unique_id()
                     << " node_info=" << proto_to_json(i);
        node_info_.insert({i.cloud_unique_id(), std::move(n)});
    }
}

std::pair<TxnErrorCode, std::string> ResourceManager::get_instance(std::shared_ptr<Transaction> txn,
                                                                   const std::string& instance_id,
                                                                   InstanceInfoPB* inst_pb) {
    std::pair<TxnErrorCode, std::string> ec {TxnErrorCode::TXN_OK, ""};
    [[maybe_unused]] auto& [code, msg] = ec;
    std::stringstream ss;

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    if (txn == nullptr) {
        std::unique_ptr<Transaction> txn0;
        TxnErrorCode err = txn_kv_->create_txn(&txn0);
        if (err != TxnErrorCode::TXN_OK) {
            code = err;
            msg = "failed to create txn";
            LOG(WARNING) << msg << " err=" << err;
            return ec;
        }
        txn.reset(txn0.release());
    }

    TxnErrorCode err = txn->get(key, &val);
    LOG(INFO) << "get instance_key=" << hex(key);

    if (err != TxnErrorCode::TXN_OK) {
        code = err;
        ss << "failed to get instance, instance_id=" << instance_id << " err=" << err;
        msg = ss.str();
        return ec;
    }

    if (inst_pb != nullptr && !inst_pb->ParseFromString(val)) {
        code = TxnErrorCode::TXN_UNIDENTIFIED_ERROR;
        msg = "failed to parse InstanceInfoPB";
        return ec;
    }

    return ec;
}

// check instance pb is valid
// check in drop nodes
// In the mode of managing cluster nodes through SQL, FE knows the master node of the current system, so it cannot delete the current master node (SQL error)
// However, in the mode of managing cluster nodes through HTTP, MS cannot know which node in the current FE cluster is the master node, so some SOP operations are required for cloud control
// 1. First, check the status of the nodes in the current FE cluster
// 2. Drop non master nodes
// 3. If you want to drop the master node, you need to find a way to switch the master node to another follower node and then drop it again
bool is_instance_valid(const InstanceInfoPB& instance) {
    // check has fe node
    for (auto& c : instance.clusters()) {
        if (c.has_type() && c.type() == ClusterPB::SQL) {
            int master = 0;
            int follower = 0;
            for (auto& n : c.nodes()) {
                if (n.node_type() == NodeInfoPB::FE_MASTER) {
                    master++;
                } else if (n.node_type() == NodeInfoPB::FE_FOLLOWER) {
                    follower++;
                }
            }
            if (master > 1 || (master == 0 && follower == 0)) {
                return false;
            }
            return true;
        }
    }
    // check others ...
    return true;
}

std::string ResourceManager::modify_nodes(const std::string& instance_id,
                                          const std::vector<NodeInfo>& to_add,
                                          const std::vector<NodeInfo>& to_del) {
    std::string msg;
    std::stringstream ss;
    DORIS_CLOUD_DEFER {
        LOG(INFO) << "modify_nodes err=" << msg;
    };

    if ((to_add.size() && to_del.size()) || (!to_add.size() && !to_del.size())) {
        msg = "to_add and to_del both empty or both not empty";
        LOG(WARNING) << msg;
        return msg;
    }

    std::unique_ptr<Transaction> txn0;
    TxnErrorCode err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << msg;
        return msg;
    }

    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    TEST_SYNC_POINT_CALLBACK("modify_nodes:get_instance", &c0, &instance);
    if (c0 != TxnErrorCode::TXN_OK) {
        msg = m0;
        return msg;
    }

    if (instance.status() == InstanceInfoPB::DELETED) {
        msg = "instance status has been set delete, plz check it";
        LOG(WARNING) << msg;
        return msg;
    }

    LOG(INFO) << "instance json=" << proto_to_json(instance);
    if (!to_add.empty()) {
        // add nodes
        // Check duplicated nodes, one node cannot deploy on multiple clusters
        // diff instance's nodes and to_add nodes
        const auto& [fe_endpoints_registered, be_endpoints_registered] =
                get_nodes_endpoint_registered(instance);
        for (auto& add_node : to_add) {
            const ClusterPB::Type type =
                    add_node.role == Role::SQL_SERVER ? ClusterPB::SQL : ClusterPB::COMPUTE;
            const auto& [c1, m1] = check_node_has_been_registered(
                    type, add_node.node_info, fe_endpoints_registered, be_endpoints_registered);
            if (c1 != MetaServiceCode::OK) {
                return m1;
            }
        }
    }
    // a vector save (origin_cluster , changed_cluster), to update ms mem
    std::vector<std::pair<ClusterPB, ClusterPB>> change_from_to_clusters;
    using modify_impl_func = std::function<std::string(const ClusterPB& c, const NodeInfo& n)>;
    using check_func = std::function<std::string(const NodeInfo& n)>;
    auto modify_func = [&](const NodeInfo& node, check_func check,
                           modify_impl_func action) -> std::string {
        std::string cluster_id = node.cluster_id;
        std::string cluster_name = node.cluster_name;

        {
            std::shared_lock l(mtx_);
            msg = check(node);
            if (msg != "") {
                return msg;
            }
        }

        LOG(INFO) << "node to modify json=" << proto_to_json(node.node_info);

        for (auto& c : instance.clusters()) {
            if ((c.has_cluster_name() && c.cluster_name() == cluster_name) ||
                (c.has_cluster_id() && c.cluster_id() == cluster_id)) {
                msg = action(c, node);
                if (msg != "") {
                    return msg;
                }
                auto now_time = std::chrono::system_clock::now();
                uint64_t time = std::chrono::duration_cast<std::chrono::seconds>(
                                        now_time.time_since_epoch())
                                        .count();
                ClusterPB modify_cluster = const_cast<std::decay_t<decltype(c)>&>(c);
                modify_cluster.set_mtime(time);
            }
        }
        return "";
    };

    // check in ms mem cache
    check_func check_to_add = [&](const NodeInfo& n) -> std::string {
        std::string err;
        std::stringstream s;
        auto [start, end] = node_info_.equal_range(n.node_info.cloud_unique_id());
        if (start == node_info_.end() || start->first != n.node_info.cloud_unique_id()) {
            return "";
        }
        for (auto it = start; it != end; ++it) {
            if (it->second.instance_id != n.instance_id) {
                // different instance, but has same cloud_unique_id
                s << "cloud_unique_id is already occupied by an instance,"
                  << " instance_id=" << it->second.instance_id
                  << " cluster_name=" << it->second.cluster_name
                  << " cluster_id=" << it->second.cluster_id
                  << " cloud_unique_id=" << n.node_info.cloud_unique_id();
                err = s.str();
                LOG(INFO) << err;
                return err;
            }
        }
        return "";
    };

    // modify kv
    modify_impl_func modify_to_add = [&](const ClusterPB& c, const NodeInfo& n) -> std::string {
        std::string err;
        std::stringstream s;
        ClusterPB copied_original_cluster;
        ClusterPB copied_cluster;
        bool is_compute_node = n.node_info.has_heartbeat_port();
        for (auto it = c.nodes().begin(); it != c.nodes().end(); ++it) {
            if (it->has_ip() && n.node_info.has_ip()) {
                std::string c_endpoint = it->ip() + ":" +
                                         (is_compute_node ? std::to_string(it->heartbeat_port())
                                                          : std::to_string(it->edit_log_port()));
                std::string n_endpoint =
                        n.node_info.ip() + ":" +
                        (is_compute_node ? std::to_string(n.node_info.heartbeat_port())
                                         : std::to_string(n.node_info.edit_log_port()));
                if (c_endpoint == n_endpoint) {
                    // replicate request, do nothing
                    return "";
                }
            }

            if (it->has_host() && n.node_info.has_host()) {
                std::string c_endpoint_host =
                        it->host() + ":" +
                        (is_compute_node ? std::to_string(it->heartbeat_port())
                                         : std::to_string(it->edit_log_port()));
                std::string n_endpoint_host =
                        n.node_info.host() + ":" +
                        (is_compute_node ? std::to_string(n.node_info.heartbeat_port())
                                         : std::to_string(n.node_info.edit_log_port()));
                if (c_endpoint_host == n_endpoint_host) {
                    // replicate request, do nothing
                    return "";
                }
            }
        }

        // add ctime and mtime
        auto& node = const_cast<std::decay_t<decltype(n.node_info)>&>(n.node_info);
        auto now_time = std::chrono::system_clock::now();
        uint64_t time =
                std::chrono::duration_cast<std::chrono::seconds>(now_time.time_since_epoch())
                        .count();
        if (!node.has_ctime()) {
            node.set_ctime(time);
        }
        node.set_mtime(time);
        copied_original_cluster.CopyFrom(c);
        auto& change_cluster = const_cast<std::decay_t<decltype(c)>&>(c);
        change_cluster.add_nodes()->CopyFrom(node);
        copied_cluster.CopyFrom(change_cluster);
        change_from_to_clusters.emplace_back(std::move(copied_original_cluster),
                                             std::move(copied_cluster));
        return "";
    };

    for (auto& it : to_add) {
        msg = modify_func(it, check_to_add, modify_to_add);
        if (msg != "") {
            LOG(WARNING) << msg;
            return msg;
        }
    }

    std::string cache_err_help_msg =
            "Ms nodes memory cache may be inconsistent, pls check registry key may be contain "
            "127.0.0.1, and call get_instance api get instance info from fdb";

    // check in ms mem cache
    check_func check_to_del = [&](const NodeInfo& n) -> std::string {
        std::string err;
        std::stringstream s;
        auto [start, end] = node_info_.equal_range(n.node_info.cloud_unique_id());
        if (start == node_info_.end() || start->first != n.node_info.cloud_unique_id()) {
            s << "can not find to drop nodes by cloud_unique_id=" << n.node_info.cloud_unique_id()
              << " instance_id=" << n.instance_id << " cluster_name=" << n.cluster_name
              << " cluster_id=" << n.cluster_id << " help Msg=" << cache_err_help_msg;
            err = s.str();
            LOG(WARNING) << err;
            return std::string("not found ,") + err;
        }

        bool found = false;
        for (auto it = start; it != end; ++it) {
            const auto& m_node = it->second.node_info;
            if (m_node.has_ip() && n.node_info.has_ip()) {
                std::string m_endpoint =
                        m_node.ip() + ":" +
                        (m_node.has_heartbeat_port() ? std::to_string(m_node.heartbeat_port())
                                                     : std::to_string(m_node.edit_log_port()));

                std::string n_endpoint = n.node_info.ip() + ":" +
                                         (n.node_info.has_heartbeat_port()
                                                  ? std::to_string(n.node_info.heartbeat_port())
                                                  : std::to_string(n.node_info.edit_log_port()));

                if (m_endpoint == n_endpoint) {
                    found = true;
                    break;
                }
            }

            if (m_node.has_host() && n.node_info.has_host()) {
                std::string m_endpoint_host =
                        m_node.host() + ":" +
                        (m_node.has_heartbeat_port() ? std::to_string(m_node.heartbeat_port())
                                                     : std::to_string(m_node.edit_log_port()));

                std::string n_endpoint_host =
                        n.node_info.host() + ":" +
                        (n.node_info.has_heartbeat_port()
                                 ? std::to_string(n.node_info.heartbeat_port())
                                 : std::to_string(n.node_info.edit_log_port()));

                if (m_endpoint_host == n_endpoint_host) {
                    found = true;
                    break;
                }
            }
        }
        if (!found) {
            s << "cloud_unique_id can not find to drop node,"
              << " instance_id=" << n.instance_id << " cluster_name=" << n.cluster_name
              << " cluster_id=" << n.cluster_id << " node_info=" << n.node_info.DebugString()
              << " help Msg=" << cache_err_help_msg;
            err = s.str();
            LOG(WARNING) << err;
            return std::string("not found ,") + err;
        }
        return "";
    };

    // modify kv
    modify_impl_func modify_to_del = [&](const ClusterPB& c, const NodeInfo& n) -> std::string {
        std::string err;
        std::stringstream s;
        ClusterPB copied_original_cluster;
        ClusterPB copied_cluster;

        bool found = false;
        int idx = -1;
        // ni: to drop node
        const auto& ni = n.node_info;
        // c.nodes: cluster registered nodes
        NodeInfoPB copy_node;
        for (auto& cn : c.nodes()) {
            idx++;
            if (cn.has_ip() && ni.has_ip()) {
                std::string cn_endpoint =
                        cn.ip() + ":" +
                        (cn.has_heartbeat_port() ? std::to_string(cn.heartbeat_port())
                                                 : std::to_string(cn.edit_log_port()));

                std::string ni_endpoint =
                        ni.ip() + ":" +
                        (ni.has_heartbeat_port() ? std::to_string(ni.heartbeat_port())
                                                 : std::to_string(ni.edit_log_port()));

                if (ni.cloud_unique_id() == cn.cloud_unique_id() && cn_endpoint == ni_endpoint) {
                    copy_node.CopyFrom(cn);
                    found = true;
                    break;
                }
            }

            if (cn.has_host() && ni.has_host()) {
                std::string cn_endpoint_host =
                        cn.host() + ":" +
                        (cn.has_heartbeat_port() ? std::to_string(cn.heartbeat_port())
                                                 : std::to_string(cn.edit_log_port()));

                std::string ni_endpoint_host =
                        ni.host() + ":" +
                        (ni.has_heartbeat_port() ? std::to_string(ni.heartbeat_port())
                                                 : std::to_string(ni.edit_log_port()));

                if (ni.cloud_unique_id() == cn.cloud_unique_id() &&
                    cn_endpoint_host == ni_endpoint_host) {
                    copy_node.CopyFrom(cn);
                    found = true;
                    break;
                }
            }
        }

        if (!found) {
            s << "failed to find node to drop,"
              << " instance_id=" << instance.instance_id() << " cluster_id=" << c.cluster_id()
              << " cluster_name=" << c.cluster_name() << " cluster=" << proto_to_json(c)
              << " help Msg =" << cache_err_help_msg;
            err = s.str();
            LOG(WARNING) << err;
            return std::string("not found ,") + err;
        }

        // check drop fe node
        if (ClusterPB::SQL == c.type() && config::enable_check_fe_drop_in_safe_time &&
            !is_sql_node_exceeded_safe_drop_time(copy_node)) {
            s << "drop fe node not in safe time, try later, node=" << copy_node.DebugString();
            err = s.str();
            LOG(WARNING) << err;
            return err;
        }
        copied_original_cluster.CopyFrom(c);
        auto& change_nodes = const_cast<std::decay_t<decltype(c.nodes())>&>(c.nodes());
        change_nodes.DeleteSubrange(idx, 1); // Remove it
        copied_cluster.CopyFrom(c);
        change_from_to_clusters.emplace_back(std::move(copied_original_cluster),
                                             std::move(copied_cluster));
        return "";
    };

    for (auto& it : to_del) {
        msg = modify_func(it, check_to_del, modify_to_del);
        if (msg != "") {
            LOG(WARNING) << msg;
            return msg;
        }
    }

    LOG(INFO) << "instance " << instance_id << " info: " << instance.DebugString();
    // here, instance has been changed, not save in fdb
    if ((!to_add.empty() || !to_del.empty()) && !is_instance_valid(instance)) {
        msg = "instance invalid, cant modify, plz check";
        LOG(WARNING) << msg;
        return msg;
    }

    InstanceKeyInfo key_info {instance_id};
    std::string key;
    std::string val;
    instance_key(key_info, &key);

    val = instance.SerializeAsString();
    if (val.empty()) {
        msg = "failed to serialize";
        return msg;
    }

    txn->put(key, val);
    LOG(INFO) << "put instance_key=" << hex(key);
    TxnErrorCode err_code = txn->commit();
    if (err_code != TxnErrorCode::TXN_OK) {
        msg = "failed to commit kv txn";
        LOG(WARNING) << msg << " err=" << err_code;
        return msg;
    }

    refresh_instance(instance_id, instance);

    return "";
}

std::pair<MetaServiceCode, std::string> ResourceManager::refresh_instance(
        const std::string& instance_id) {
    LOG(INFO) << "begin to refresh instance, instance_id=" << instance_id << " seq=" << ++seq;
    std::pair<MetaServiceCode, std::string> ret0 {MetaServiceCode::OK, "OK"};
    auto& [code, msg] = ret0;
    DORIS_CLOUD_DEFER {
        LOG(INFO) << (std::get<0>(ret0) == MetaServiceCode::OK ? "succ to " : "failed to ")
                  << "refresh_instance, instance_id=" << instance_id
                  << " code=" << std::get<0>(ret0) << " msg=" << std::get<1>(ret0);
    };

    std::unique_ptr<Transaction> txn0;
    TxnErrorCode err = txn_kv_->create_txn(&txn0);
    if (err != TxnErrorCode::TXN_OK) {
        code = MetaServiceCode::KV_TXN_CREATE_ERR;
        msg = "failed to create txn";
        LOG(WARNING) << msg << " err=" << err;
        return ret0;
    }
    std::shared_ptr<Transaction> txn(txn0.release());
    InstanceInfoPB instance;
    auto [c0, m0] = get_instance(txn, instance_id, &instance);
    if (c0 != TxnErrorCode::TXN_OK) {
        code = cast_as<ErrCategory::READ>(c0);
        msg = m0;
        return ret0;
    }

    refresh_instance(instance_id, instance);
    LOG(INFO) << "finish refreshing instance, instance_id=" << instance_id << " seq=" << seq;

    return ret0;
}

void ResourceManager::refresh_instance(const std::string& instance_id,
                                       const InstanceInfoPB& instance) {
    std::lock_guard l(mtx_);
    for (auto i = node_info_.begin(); i != node_info_.end();) {
        if (i->second.instance_id != instance_id) {
            ++i;
            continue;
        }
        i = node_info_.erase(i);
    }
    for (int i = 0; i < instance.clusters_size(); ++i) {
        add_cluster_to_index_no_lock(instance_id, instance.clusters(i));
    }

    if (instance.has_multi_version_status()) {
        instance_multi_version_status_[instance_id] = instance.multi_version_status();
    } else {
        instance_multi_version_status_.erase(instance_id);
    }
}

bool ResourceManager::is_version_read_enabled(std::string_view instance_id) const {
    MultiVersionStatus status = get_instance_multi_version_status(instance_id);
    return status == MultiVersionStatus::MULTI_VERSION_READ_WRITE ||
           status == MultiVersionStatus::MULTI_VERSION_ENABLED;
}

bool ResourceManager::is_version_write_enabled(std::string_view instance_id) const {
    MultiVersionStatus status = get_instance_multi_version_status(instance_id);
    return status == MultiVersionStatus::MULTI_VERSION_WRITE_ONLY ||
           status == MultiVersionStatus::MULTI_VERSION_READ_WRITE ||
           status == MultiVersionStatus::MULTI_VERSION_ENABLED;
}

MultiVersionStatus ResourceManager::get_instance_multi_version_status(
        std::string_view instance_id) const {
    std::shared_lock lock(mtx_);
    auto it = instance_multi_version_status_.find(std::string(instance_id));
    if (it != instance_multi_version_status_.end()) {
        return it->second;
    }

    // Default to disabled if not found or not set
    return MultiVersionStatus::MULTI_VERSION_DISABLED;
}

std::pair<MetaServiceCode, std::string> ResourceManager::validate_sub_clusters(
        const std::vector<std::string>& check_clusters,
        const std::vector<ClusterPB>& clusters_in_instance) {
    std::string msg;
    for (const auto& check_cluster : check_clusters) {
        auto it = std::find_if(clusters_in_instance.begin(), clusters_in_instance.end(),
                               [&check_cluster](const auto& cluster) {
                                   return check_cluster == cluster.cluster_name();
                               });
        if (it == clusters_in_instance.end()) {
            msg = "sub cluster " + check_cluster +
                  " not been added in instance, plz add it before create virtual cluster";
            LOG(WARNING) << msg;
            return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
        }
        // sub cluster type must be COMPUTE
        if (ClusterPB::COMPUTE != it->type()) {
            msg = "sub cluster " + check_cluster + " 's type must be eq COMPUTE";
            LOG(WARNING) << msg;
            return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
        }
        // check one cluster can only be mounted by one virtual cluster at most.
        for (const auto& cluster_in_instance : clusters_in_instance) {
            if (ClusterPB::VIRTUAL == cluster_in_instance.type()) {
                auto it = std::find_if(
                        cluster_in_instance.cluster_names().begin(),
                        cluster_in_instance.cluster_names().end(),
                        [&check_cluster](const auto& cluster) { return check_cluster == cluster; });
                if (it != cluster_in_instance.cluster_names().end()) {
                    msg = "sub cluster " + check_cluster +
                          " has been add by other vcg=" + cluster_in_instance.cluster_name();
                    LOG(WARNING) << msg;
                    return std::make_pair(MetaServiceCode::INVALID_ARGUMENT, msg);
                }
            }
        }
    }

    return std::make_pair(MetaServiceCode::OK, "");
}

} // namespace doris::cloud
