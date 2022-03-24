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

#ifndef DORIS_BE_SRC_COMMON_UTIL_THRIFT_SERVER_H
#define DORIS_BE_SRC_COMMON_UTIL_THRIFT_SERVER_H

#include <thrift/TProcessor.h>
#include <thrift/server/TServer.h>

#include <mutex>
#include <thread>
#include <unordered_map>

#include "common/status.h"
#include "util/metrics.h"

namespace doris {
// Utility class for all Thrift servers. Runs a TNonblockingServer(default) or a
// TThreadPoolServer with, by default, 2 worker threads, that exposes the interface
// described by a user-supplied TProcessor object.
// If TNonblockingServer is used, client must use TFramedTransport.
// If TThreadPoolServer is used, client must use TSocket as transport.
class ThriftServer {
public:
    // An opaque identifier for the current session, which identifies a client connection.
    typedef std::string SessionKey;

    // Interface class for receiving session creation / termination events.
    class SessionHandlerIf {
    public:
        // Called when a session is established (when a client connects).
        virtual void session_start(const SessionKey& session_key) = 0;

        // Called when a session is terminated (when a client closes the connection).
        // After this callback returns, the memory session_key references is no longer valid
        // and clients must not refer to it again.
        virtual void session_end(const SessionKey& session_key) = 0;
    };

    static const int DEFAULT_WORKER_THREADS = 2;

    // There are 3 servers supported by Thrift with different threading models.
    // THREAD_POOL  -- Allocates a fixed number of threads. A thread is used by a
    //                connection until it closes.
    // THREADED     -- Allocates 1 thread per connection, as needed.
    // NON_BLOCKING -- Threads are allocated to a connection only when the server
    //                is working on behalf of the connection.
    enum ServerType { THREAD_POOL = 0, THREADED, NON_BLOCKING };

    // Creates, but does not start, a new server on the specified port
    // that exports the supplied interface.
    //  - name: human-readable name of this server. Should not contain spaces
    //  - processor: Thrift processor to handle RPCs
    //  - port: The port the server will listen for connections on
    //  - num_worker_threads: the number of worker threads to use in any thread pool
    //  - server_type: the type of IO strategy this server should employ
    ThriftServer(const std::string& name,
                 const std::shared_ptr<apache::thrift::TProcessor>& processor, int port,
                 int num_worker_threads = DEFAULT_WORKER_THREADS,
                 ServerType server_type = THREADED);

    ~ThriftServer() {}

    int port() const { return _port; }

    void stop();
    // Blocks until the server stops and exits its main thread.
    void join();

    // FOR TESTING ONLY; stop the server and block until the server is stopped; use it
    // only if it is a Threaded server.
    void stop_for_testing();

    // Starts the main server thread. Once this call returns, clients
    // may connect to this server and issue RPCs. May not be called more
    // than once.
    Status start();

    // Sets the session handler which receives events when sessions are created or closed.
    void set_session_handler(SessionHandlerIf* session) { _session_handler = session; }

    // Returns a unique identifier for the current session. A session is
    // identified with the lifetime of a socket connection to this server.
    // It is only safe to call this method during a Thrift processor RPC
    // implementation. Otherwise, the result of calling this method is undefined.
    // It is also only safe to reference the returned value during an RPC method.
    static SessionKey* get_thread_session_key();

private:
    // True if the server has been successfully started, for internal use only
    bool _started;

    // The port on which the server interface is exposed
    int _port;

    // How many worker threads to use to serve incoming requests
    // (requests are queued if no thread is immediately available)
    int _num_worker_threads;

    // ThreadPool or NonBlocking server
    ServerType _server_type;

    // User-specified identifier that shows up in logs
    const std::string _name;

    // Thread that runs the TNonblockingServer::serve loop
    std::unique_ptr<std::thread> _server_thread;

    // Thrift housekeeping
    std::unique_ptr<apache::thrift::server::TServer> _server;
    std::shared_ptr<apache::thrift::TProcessor> _processor;

    // If not nullptr, called when session events happen. Not owned by us.
    SessionHandlerIf* _session_handler;

    // Protects _session_keys
    std::mutex _session_keys_lock;

    // Map of active session keys to shared_ptr containing that key; when a key is
    // removed it is automatically freed.
    typedef std::unordered_map<SessionKey*, std::shared_ptr<SessionKey>> SessionKeySet;
    SessionKeySet _session_keys;

    // Helper class which monitors starting servers. Needs access to internal members, and
    // is not used outside of this class.
    class ThriftServerEventProcessor;
    friend class ThriftServerEventProcessor;

    std::shared_ptr<MetricEntity> _thrift_server_metric_entity;
    // Number of currently active connections
    IntGauge* thrift_current_connections;
    // Total connections made over the lifetime of this server
    IntCounter* thrift_connections_total;
};

} // namespace doris

#endif
