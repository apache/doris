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

#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TServer.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>

#include <boost/foreach.hpp>
#include <memory>
#include <sstream>

#include "common/logging.h"
#include "gen_cpp/FrontendService.h"
#include "util/container_util.hpp"
#include "util/doris_metrics.h"
#include "util/network_util.h"
#include "util/thrift_util.h"

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

Status ClientCacheHelper::get_client(const TNetworkAddress& hostport, client_factory factory_method,
                                     void** client_key, int timeout_ms) {
    boost::lock_guard<boost::mutex> lock(_lock);
    //VLOG_RPC << "get_client(" << hostport << ")";
    ClientCacheMap::iterator cache_entry = _client_cache.find(hostport);

    if (cache_entry == _client_cache.end()) {
        cache_entry = _client_cache.insert(std::make_pair(hostport, std::list<void*>())).first;
        DCHECK(cache_entry != _client_cache.end());
    }

    std::list<void*>& info_list = cache_entry->second;

    if (!info_list.empty()) {
        *client_key = info_list.front();
        VLOG_RPC << "get_client(): cached client for " << hostport;
        info_list.pop_front();
    } else {
        RETURN_IF_ERROR(create_client(hostport, factory_method, client_key, timeout_ms));
    }

    _client_map[*client_key]->set_send_timeout(timeout_ms);
    _client_map[*client_key]->set_recv_timeout(timeout_ms);

    if (_metrics_enabled) {
        thrift_used_clients->increment(1);
    }

    return Status::OK();
}

Status ClientCacheHelper::reopen_client(client_factory factory_method, void** client_key,
                                        int timeout_ms) {
    boost::lock_guard<boost::mutex> lock(_lock);
    ClientMap::iterator i = _client_map.find(*client_key);
    DCHECK(i != _client_map.end());
    ThriftClientImpl* info = i->second;
    const std::string ipaddress = info->ipaddress();
    int port = info->port();

    info->close();

    // TODO: Thrift TBufferedTransport cannot be re-opened after Close() because it does
    // not clean up internal buffers it reopens. To work around this issue, create a new
    // client instead.
    _client_map.erase(*client_key);
    delete info;
    *client_key = NULL;

    if (_metrics_enabled) {
        thrift_opened_clients->increment(-1);
    }

    RETURN_IF_ERROR(create_client(make_network_address(ipaddress, port), factory_method, client_key,
                                  timeout_ms));

    _client_map[*client_key]->set_send_timeout(timeout_ms);
    _client_map[*client_key]->set_recv_timeout(timeout_ms);
    return Status::OK();
}

Status ClientCacheHelper::create_client(const TNetworkAddress& hostport,
                                        client_factory factory_method, void** client_key,
                                        int timeout_ms) {
    std::unique_ptr<ThriftClientImpl> client_impl(factory_method(hostport, client_key));
    //VLOG_CONNECTION << "create_client(): adding new client for "
    //                << client_impl->ipaddress() << ":" << client_impl->port();

    client_impl->set_conn_timeout(config::thrift_connect_timeout_seconds * 1000);

    Status status = client_impl->open();

    if (!status.ok()) {
        *client_key = NULL;
        return status;
    }

    // Because the client starts life 'checked out', we don't add it to the cache map
    _client_map[*client_key] = client_impl.release();

    if (_metrics_enabled) {
        thrift_opened_clients->increment(1);
    }

    return Status::OK();
}

void ClientCacheHelper::release_client(void** client_key) {
    DCHECK(*client_key != NULL) << "Trying to release NULL client";
    boost::lock_guard<boost::mutex> lock(_lock);
    ClientMap::iterator client_map_entry = _client_map.find(*client_key);
    DCHECK(client_map_entry != _client_map.end());
    ThriftClientImpl* info = client_map_entry->second;
    ClientCacheMap::iterator j =
            _client_cache.find(make_network_address(info->ipaddress(), info->port()));
    DCHECK(j != _client_cache.end());

    if (_max_cache_size_per_host >= 0 && j->second.size() >= _max_cache_size_per_host) {
        // cache of this host is full, close this client connection and remove if from _client_map
        info->close();
        _client_map.erase(*client_key);
        delete info;

        if (_metrics_enabled) {
            thrift_opened_clients->increment(-1);
        }
    } else {
        j->second.push_back(*client_key);
    }

    if (_metrics_enabled) {
        thrift_used_clients->increment(-1);
    }

    *client_key = NULL;
}

void ClientCacheHelper::close_connections(const TNetworkAddress& hostport) {
    boost::lock_guard<boost::mutex> lock(_lock);
    ClientCacheMap::iterator cache_entry = _client_cache.find(hostport);

    if (cache_entry == _client_cache.end()) {
        return;
    }

    VLOG_RPC << "Invalidating all " << cache_entry->second.size() << " clients for: " << hostport;
    BOOST_FOREACH (void* client_key, cache_entry->second) {
        ClientMap::iterator client_map_entry = _client_map.find(client_key);
        DCHECK(client_map_entry != _client_map.end());
        ThriftClientImpl* info = client_map_entry->second;
        info->close();
        _client_map.erase(client_key);
        delete info;
    }
}

std::string ClientCacheHelper::debug_string() {
    std::stringstream out;
    out << "ClientCacheHelper(#hosts=" << _client_cache.size() << " [";

    for (ClientCacheMap::iterator i = _client_cache.begin(); i != _client_cache.end(); ++i) {
        if (i != _client_cache.begin()) {
            out << " ";
        }

        out << i->first << ":" << i->second.size();
    }

    out << "])";
    return out.str();
}

void ClientCacheHelper::test_shutdown() {
    std::vector<TNetworkAddress> hostports;
    {
        boost::lock_guard<boost::mutex> lock(_lock);
        BOOST_FOREACH (const ClientCacheMap::value_type& i, _client_cache) {
            hostports.push_back(i.first);
        }
    }

    for (std::vector<TNetworkAddress>::iterator it = hostports.begin(); it != hostports.end();
         ++it) {
        close_connections(*it);
    }
}

void ClientCacheHelper::init_metrics(const std::string& name) {
    // Not strictly needed if init_metrics is called before any cache
    // usage, but ensures that _metrics_enabled is published.
    boost::lock_guard<boost::mutex> lock(_lock);

    _thrift_client_metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("thrift_client.") + name, {{"name", name}});
    INT_GAUGE_METRIC_REGISTER(_thrift_client_metric_entity, thrift_used_clients);
    INT_GAUGE_METRIC_REGISTER(_thrift_client_metric_entity, thrift_opened_clients);

    _metrics_enabled = true;
}

} // namespace doris
