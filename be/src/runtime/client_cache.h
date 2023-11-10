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

#include <gen_cpp/BackendService.h>     // IWYU pragma: keep
#include <gen_cpp/FrontendService.h>    // IWYU pragma: keep
#include <gen_cpp/TPaloBrokerService.h> // IWYU pragma: keep
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <string.h>
#include <unistd.h>

#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <ostream>
#include <string>
#include <typeinfo>
#include <unordered_map>

#include "common/config.h"
#include "common/status.h"
#include "util/hash_util.hpp"
#include "util/metrics.h"
#include "util/thrift_client.h"
#include "util/thrift_server.h"

namespace doris {
// Helper class which implements the majority of the caching
// functionality without using templates (i.e. pointers to the
// superclass of all ThriftClients and a void* for the key).
//
// The user of this class only sees RPC proxy classes, but we have
// to track the ThriftClient to manipulate the underlying
// transport. To do this, we maintain a map from an opaque 'key'
// pointer type to the client implementation. We actually know the
// type of the pointer (it's the type parameter to ClientCache), but
// we deliberately avoid using it so that this entire class doesn't
// get inlined every time it gets used.
//
// This class is thread-safe.
//
// TODO: shut down clients in the background if they don't get used for a period of time
// TODO: in order to reduce locking overhead when getting/releasing clients,
// add call to hand back pointer to list stored in ClientCache and add separate lock
// to list (or change to lock-free list)
// TODO: reduce locking overhead and by adding per-address client caches, each with its
// own lock.
// TODO: More graceful handling of clients that have failed (maybe better
// handled by a smart-wrapper of the interface object).
// TODO: limits on total number of clients, and clients per-backend
class ClientCacheHelper {
public:
    ~ClientCacheHelper();
    // Callback method which produces a client object when one cannot be
    // found in the cache. Supplied by the ClientCache wrapper.
    using ClientFactory =
            std::function<ThriftClientImpl*(const TNetworkAddress& hostport, void** client_key)>;

    // Return client for specific host/port in 'client'. If a client
    // is not available, the client parameter is set to nullptr.
    Status get_client(const TNetworkAddress& hostport, ClientFactory& factory_method,
                      void** client_key, int timeout_ms);

    // Close and delete the underlying transport and remove the client from _client_map.
    // Return a new client connecting to the same host/port.
    // Return an error status and set client_key to nullptr if a new client cannot
    // created.
    Status reopen_client(ClientFactory& factory_method, void** client_key, int timeout_ms);

    // Return a client to the cache, without closing it, and set *client_key to nullptr.
    void release_client(void** client_key);

    std::string debug_string();

    void init_metrics(const std::string& name);

private:
    template <class T>
    friend class ClientCache;
    // Private constructor so that only ClientCache can instantiate this class.
    ClientCacheHelper() : _metrics_enabled(false), _max_cache_size_per_host(-1) {}

    ClientCacheHelper(int max_cache_size_per_host)
            : _metrics_enabled(false), _max_cache_size_per_host(max_cache_size_per_host) {}

    // Protects all member variables
    // TODO: have more fine-grained locks or use lock-free data structures,
    // this isn't going to scale for a high request rate
    std::mutex _lock;

    // map from (host, port) to list of client keys for that address
    using ClientCacheMap = std::unordered_map<TNetworkAddress, std::list<void*>>;
    ClientCacheMap _client_cache;

    // if cache not found, set client_key as nullptr
    void _get_client_from_cache(const TNetworkAddress& hostport, void** client_key);

    // Map from client key back to its associated ThriftClientImpl transport
    using ClientMap = std::unordered_map<void*, ThriftClientImpl*>;
    ClientMap _client_map;

    bool _metrics_enabled;

    // max connections per host in this cache, -1 means unlimited
    int _max_cache_size_per_host;

    std::shared_ptr<MetricEntity> _thrift_client_metric_entity;

    // Number of clients 'checked-out' from the cache
    IntGauge* thrift_used_clients;

    // Total clients in the cache, including those in use
    IntGauge* thrift_opened_clients;

    // Create a new client for specific host/port in 'client' and put it in _client_map
    Status _create_client(const TNetworkAddress& hostport, ClientFactory& factory_method,
                          void** client_key, int timeout_ms);
};

template <class T>
class ClientCache;

// A scoped client connection to help manage clients from a client cache.
//
// Example:
//   {
//     DorisInternalServiceConnection client(cache, address, &status);
//     try {
//       client->TransmitData(...);
//     } catch (TTransportException& e) {
//       // Retry
//       RETURN_IF_ERROR(client.Reopen());
//       client->TransmitData(...);
//     }
//   }
// ('client' is released back to cache upon destruction.)
template <class T>
class ClientConnection {
public:
    ClientConnection(ClientCache<T>* client_cache, const TNetworkAddress& address, Status* status)
            : ClientConnection(client_cache, address, 0, status, 3) {}

    ClientConnection(ClientCache<T>* client_cache, const TNetworkAddress& address, int timeout_ms,
                     Status* status, int max_retries = 3)
            : _client_cache(client_cache), _client(nullptr) {
        int num_retries = 0;
        do {
            *status = _client_cache->get_client(address, &_client, timeout_ms);
            if (status->ok()) {
                DCHECK(_client != nullptr);
                break;
            }
            DCHECK(_client == nullptr);
            if (num_retries++ < max_retries) {
                // exponential backoff retry with starting delay of 500ms
                usleep(500000 * (1 << num_retries));
                LOG(INFO) << "Failed to get client from cache: " << status->to_string()
                          << ", retrying[" << num_retries << "]...";
            }
        } while (num_retries < max_retries);
    }

    ~ClientConnection() {
        if (_client != nullptr) {
            _client_cache->release_client(&_client);
        }
    }

    Status reopen(int timeout_ms) { return _client_cache->reopen_client(&_client, timeout_ms); }

    Status reopen() { return _client_cache->reopen_client(&_client, 0); }

    inline bool is_alive() { return _client != nullptr; }

    T* operator->() const { return _client; }

private:
    ClientCache<T>* _client_cache;
    T* _client;
};

// Generic cache of Thrift clients for a given service type.
// This class is thread-safe.
template <class T>
class ClientCache {
public:
    using Client = ThriftClient<T>;

    ClientCache() {
        _client_factory =
                std::bind<ThriftClientImpl*>(std::mem_fn(&ClientCache::make_client), this,
                                             std::placeholders::_1, std::placeholders::_2);
    }

    ClientCache(int max_cache_size) : _client_cache_helper(max_cache_size) {
        _client_factory =
                std::bind<ThriftClientImpl*>(std::mem_fn(&ClientCache::make_client), this,
                                             std::placeholders::_1, std::placeholders::_2);
    }

    // Helper method which returns a debug string
    std::string debug_string() { return _client_cache_helper.debug_string(); }

    // Adds metrics for this cache.
    // The metrics have an identification by the 'name' argument
    // (which should not end in a period).
    // Must be called before the cache is used, otherwise the metrics might be wrong
    void init_metrics(const std::string& name) { _client_cache_helper.init_metrics(name); }

private:
    friend class ClientConnection<T>;

    // Most operations in this class are thin wrappers around the
    // equivalent in ClientCacheHelper, which is a non-templated cache
    // to avoid inlining lots of code wherever this cache is used.
    ClientCacheHelper _client_cache_helper;

    // Function pointer, bound to make_client, which produces clients when the cache is empty
    using ClientFactory = ClientCacheHelper::ClientFactory;
    ClientFactory _client_factory;

    // Obtains a pointer to a Thrift interface object (of type T),
    // backed by a live transport which is already open. Returns
    // Status::OK() unless there was an error opening the transport.
    Status get_client(const TNetworkAddress& hostport, T** iface, int timeout_ms) {
        return _client_cache_helper.get_client(hostport, _client_factory,
                                               reinterpret_cast<void**>(iface), timeout_ms);
    }

    // Close and delete the underlying transport. Return a new client connecting to the
    // same host/port.
    // Return an error status if a new connection cannot be established and *client will be
    // nullptr in that case.
    Status reopen_client(T** client, int timeout_ms) {
        return _client_cache_helper.reopen_client(_client_factory, reinterpret_cast<void**>(client),
                                                  timeout_ms);
    }

    // Return the client to the cache and set *client to nullptr.
    void release_client(T** client) {
        return _client_cache_helper.release_client(reinterpret_cast<void**>(client));
    }

    // Factory method to produce a new ThriftClient<T> for the wrapped cache
    ThriftClientImpl* make_client(const TNetworkAddress& hostport, void** client_key) {
        static ThriftServer::ServerType server_type = get_thrift_server_type();
        Client* client = new Client(hostport.hostname, hostport.port, server_type);
        *client_key = reinterpret_cast<void*>(client->iface());
        return client;
    }

    // since service type is multiple, we should set thrift server type here for be thrift client
    ThriftServer::ServerType get_thrift_server_type() {
        auto& thrift_server_type = config::thrift_server_type_of_fe;
        std::transform(thrift_server_type.begin(), thrift_server_type.end(),
                       thrift_server_type.begin(), [](auto c) { return std::toupper(c); });
        if (strcmp(typeid(T).name(), "N5doris21FrontendServiceClientE") == 0 &&
            thrift_server_type == "THREADED_SELECTOR") {
            return ThriftServer::ServerType::NON_BLOCKING;
        } else {
            return ThriftServer::ServerType::THREADED;
        }
    }
};

// Doris backend client cache, used by a backend to send requests
// to any other backend.
using BackendServiceClientCache = ClientCache<BackendServiceClient>;
using BackendServiceConnection = ClientConnection<BackendServiceClient>;

using FrontendServiceClientCache = ClientCache<FrontendServiceClient>;
using FrontendServiceConnection = ClientConnection<FrontendServiceClient>;

using BrokerServiceClientCache = ClientCache<TPaloBrokerServiceClient>;
using BrokerServiceConnection = ClientConnection<TPaloBrokerServiceClient>;

} // namespace doris
