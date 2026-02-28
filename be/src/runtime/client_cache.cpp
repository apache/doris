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

#include "runtime/client_cache.h"

#include <memory>
#include <utility>

#include "common/logging.h"
#include "runtime/exec_env.h"
#include "util/dns_cache.h"
#include "util/doris_metrics.h"
#include "util/network_util.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_3ARG(thrift_used_clients, MetricUnit::NOUNIT,
                                   "Number of clients 'checked-out' from the cache");
DEFINE_GAUGE_METRIC_PROTOTYPE_3ARG(thrift_opened_clients, MetricUnit::NOUNIT,
                                   "Total clients in the cache, including those in use");

ClientCacheHelper::~ClientCacheHelper() {
    for (auto& it : _client_map) {
        delete it.second;
    }
}

void ClientCacheHelper::_get_client_from_cache(const TNetworkAddress& hostport,
                                               const std::string& resolved_ip, void** client_key) {
    *client_key = nullptr;
    std::lock_guard<std::mutex> lock(_lock);
    //VLOG_RPC << "get_client(" << hostport << ")";
    auto cache_entry = _client_cache.find(hostport);

    if (cache_entry == _client_cache.end()) {
        cache_entry =
                _client_cache.insert(std::make_pair(hostport, std::list<CachedClient>())).first;
        DCHECK(cache_entry != _client_cache.end());
    }

    std::list<CachedClient>& info_list = cache_entry->second;
    // Find a cached client with matching resolved IP
    for (auto it = info_list.begin(); it != info_list.end(); ++it) {
        if (it->resolved_ip == resolved_ip) {
            *client_key = it->client_key;
            VLOG_RPC << "get_client(): cached client for " << hostport << " with ip "
                     << resolved_ip;
            info_list.erase(it);
            return;
        }
    }

    // No matching client found. Clear all cached clients with stale IPs for this hostport.
    // These clients were created with old resolved IPs and should be closed.
    for (auto& cached_client : info_list) {
        auto client_map_entry = _client_map.find(cached_client.client_key);
        if (client_map_entry != _client_map.end()) {
            ThriftClientImpl* client_to_close = client_map_entry->second;
            client_to_close->close();
            delete client_to_close;
            _client_map.erase(client_map_entry);
            _client_hostport_map.erase(cached_client.client_key);
            if (_metrics_enabled) {
                thrift_opened_clients->increment(-1);
            }
        }
    }
    info_list.clear();
}

Status ClientCacheHelper::get_client(const TNetworkAddress& hostport, ClientFactory& factory_method,
                                     void** client_key, int timeout_ms) {
    // Resolve hostname to IP address via DNS cache.
    // If the hostname is already an IP address, DNS cache will return it directly.
    std::string resolved_ip;
    Status dns_status = ExecEnv::GetInstance()->dns_cache()->get(hostport.hostname, &resolved_ip);
    if (!dns_status.ok() || resolved_ip.empty()) {
        return Status::InternalError("Failed to resolve hostname {} to IP address: {}",
                                     hostport.hostname, dns_status.to_string());
    }

    // Try to get a cached client with matching resolved IP
    _get_client_from_cache(hostport, resolved_ip, client_key);
    if (*client_key == nullptr) {
        // No cached client with matching IP, create a new one using the resolved IP
        RETURN_IF_ERROR(
                _create_client(hostport, resolved_ip, factory_method, client_key, timeout_ms));
    }

    if (_metrics_enabled) {
        thrift_used_clients->increment(1);
    }

    return Status::OK();
}

Status ClientCacheHelper::reopen_client(ClientFactory& factory_method, void** client_key,
                                        int timeout_ms) {
    DCHECK(*client_key != nullptr) << "Trying to reopen nullptr client";
    ThriftClientImpl* client_to_close = nullptr;
    TNetworkAddress hostport;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto client_map_entry = _client_map.find(*client_key);
        DCHECK(client_map_entry != _client_map.end());
        client_to_close = client_map_entry->second;

        // Get the original hostport (with hostname) for this client
        auto hostport_entry = _client_hostport_map.find(*client_key);
        DCHECK(hostport_entry != _client_hostport_map.end());
        hostport = hostport_entry->second;
    }

    client_to_close->close();

    // TODO: Thrift TBufferedTransport cannot be re-opened after Close() because it does
    // not clean up internal buffers it reopens. To work around this issue, create a new
    // client instead.
    {
        std::lock_guard<std::mutex> lock(_lock);
        _client_map.erase(*client_key);
        _client_hostport_map.erase(*client_key);
    }
    delete client_to_close;
    *client_key = nullptr;

    if (_metrics_enabled) {
        thrift_opened_clients->increment(-1);
    }

    // Re-resolve hostname to IP address
    std::string resolved_ip;
    Status dns_status = ExecEnv::GetInstance()->dns_cache()->get(hostport.hostname, &resolved_ip);
    if (!dns_status.ok() || resolved_ip.empty()) {
        return Status::InternalError("Failed to resolve hostname {} to IP address: {}",
                                     hostport.hostname, dns_status.to_string());
    }

    RETURN_IF_ERROR(_create_client(hostport, resolved_ip, factory_method, client_key, timeout_ms));

    return Status::OK();
}

Status ClientCacheHelper::_create_client(const TNetworkAddress& hostport,
                                         const std::string& resolved_ip,
                                         ClientFactory& factory_method, void** client_key,
                                         int timeout_ms) {
    // Create a new TNetworkAddress with the resolved IP instead of hostname.
    // This ensures that all client connections are made using IP addresses.
    TNetworkAddress addr_with_ip;
    addr_with_ip.hostname = resolved_ip;
    addr_with_ip.port = hostport.port;

    std::unique_ptr<ThriftClientImpl> client_impl(factory_method(addr_with_ip, client_key));
    //VLOG_CONNECTION << "create_client(): adding new client for "
    //                << client_impl->ipaddress() << ":" << client_impl->port();

    client_impl->set_conn_timeout(config::thrift_connect_timeout_seconds * 1000);

    Status status = client_impl->open_with_retry(config::thrift_client_open_num_tries, 100);

    if (!status.ok()) {
        *client_key = nullptr;
        return status;
    }

    DCHECK(*client_key != nullptr);
    // In thrift, timeout == 0, means wait infinitely, so that it should not happen.
    // See https://github.com/apache/thrift/blob/master/lib/cpp/src/thrift/transport/TSocket.cpp.
    // There is some code like this:      int ret = THRIFT_POLL(fds, 2, (recvTimeout_ == 0) ? -1 : recvTimeout_);
    // If the developer missed to set the timeout, we should use default timeout, not infinitely.
    // See https://linux.die.net/man/2/poll. Specifying a negative value in timeout means an infinite timeout.
    client_impl->set_send_timeout(timeout_ms == 0 ? config::thrift_rpc_timeout_ms : timeout_ms);
    client_impl->set_recv_timeout(timeout_ms == 0 ? config::thrift_rpc_timeout_ms : timeout_ms);

    {
        std::lock_guard<std::mutex> lock(_lock);
        // Because the client starts life 'checked out', we don't add it to the cache map
        DCHECK(_client_map.count(*client_key) == 0);
        _client_map[*client_key] = client_impl.release();
        // Store the original hostport (with hostname) for this client
        _client_hostport_map[*client_key] = hostport;
    }

    if (_metrics_enabled) {
        thrift_opened_clients->increment(1);
    }

    return Status::OK();
}

void ClientCacheHelper::release_client(void** client_key) {
    DCHECK(*client_key != nullptr) << "Trying to release nullptr client";
    ThriftClientImpl* client_to_close = nullptr;
    {
        std::lock_guard<std::mutex> lock(_lock);
        auto client_map_entry = _client_map.find(*client_key);
        DCHECK(client_map_entry != _client_map.end());
        ThriftClientImpl* client = client_map_entry->second;

        // Get the original hostport (with hostname) for this client
        auto hostport_entry = _client_hostport_map.find(*client_key);
        DCHECK(hostport_entry != _client_hostport_map.end());
        const TNetworkAddress& hostport = hostport_entry->second;

        auto cache_list = _client_cache.find(hostport);
        DCHECK(cache_list != _client_cache.end());
        if (_max_cache_size_per_host >= 0 &&
            cache_list->second.size() >= _max_cache_size_per_host) {
            // cache of this host is full, close this client connection and remove from maps
            _client_map.erase(*client_key);
            _client_hostport_map.erase(*client_key);
            client_to_close = client;
        } else {
            // Store the client with its resolved IP address
            CachedClient cached_client;
            cached_client.client_key = *client_key;
            cached_client.resolved_ip = client->ipaddress();
            cache_list->second.push_back(cached_client);
            // There is no need to close client if we put it to cache list.
            client_to_close = nullptr;
        }
    }

    if (client_to_close != nullptr) {
        client_to_close->close();
        delete client_to_close;
        if (_metrics_enabled) {
            thrift_opened_clients->increment(-1);
        }
    }

    if (_metrics_enabled) {
        thrift_used_clients->increment(-1);
    }

    *client_key = nullptr;
}

std::string ClientCacheHelper::debug_string() {
    std::stringstream out;
    out << "ClientCacheHelper(#hosts=" << _client_cache.size() << " [";

    bool isfirst = true;
    for (const auto& [endpoint, client_keys] : _client_cache) {
        if (!isfirst) {
            out << " ";
            isfirst = false;
        }
        out << endpoint << ":" << client_keys.size();
    }

    out << "])";
    return out.str();
}

void ClientCacheHelper::init_metrics(const std::string& name) {
    // Not strictly needed if init_metrics is called before any cache
    // usage, but ensures that _metrics_enabled is published.
    std::lock_guard<std::mutex> lock(_lock);

    _thrift_client_metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("thrift_client.") + name, {{"name", name}});
    INT_GAUGE_METRIC_REGISTER(_thrift_client_metric_entity, thrift_used_clients);
    INT_GAUGE_METRIC_REGISTER(_thrift_client_metric_entity, thrift_opened_clients);

    _metrics_enabled = true;
}

} // namespace doris
