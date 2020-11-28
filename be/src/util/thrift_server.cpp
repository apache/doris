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

#include "util/thrift_server.h"

#include <thrift/concurrency/PosixThreadFactory.h>
#include <thrift/concurrency/Thread.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>

#include <boost/thread.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <sstream>

#include "util/doris_metrics.h"

namespace doris {

DEFINE_GAUGE_METRIC_PROTOTYPE_3ARG(thrift_current_connections, MetricUnit::CONNECTIONS,
                                   "Number of currently active connections");
DEFINE_COUNTER_METRIC_PROTOTYPE_3ARG(thrift_connections_total, MetricUnit::CONNECTIONS,
                                     "Total connections made over the lifetime of this server");

// Helper class that starts a server in a separate thread, and handles
// the inter-thread communication to monitor whether it started
// correctly.
class ThriftServer::ThriftServerEventProcessor
        : public apache::thrift::server::TServerEventHandler {
public:
    ThriftServerEventProcessor(ThriftServer* thrift_server)
            : _thrift_server(thrift_server), _signal_fired(false) {}

    // friendly to code style
    virtual ~ThriftServerEventProcessor() {}

    // Called by TNonBlockingServer when server has acquired its resources and is ready to
    // serve, and signals to StartAndWaitForServer that start-up is finished.
    // From TServerEventHandler.
    virtual void preServe();

    // Called when a client connects; we create per-client state and call any
    // SessionHandlerIf handler.
    virtual void* createContext(boost::shared_ptr<apache::thrift::protocol::TProtocol> input,
                                boost::shared_ptr<apache::thrift::protocol::TProtocol> output);

    // Called when a client starts an RPC; we set the thread-local session key.
    virtual void processContext(void* context,
                                boost::shared_ptr<apache::thrift::transport::TTransport> output);

    // Called when a client disconnects; we call any SessionHandlerIf handler.
    virtual void deleteContext(void* serverContext,
                               boost::shared_ptr<apache::thrift::protocol::TProtocol> input,
                               boost::shared_ptr<apache::thrift::protocol::TProtocol> output);

    // Waits for a timeout of TIMEOUT_MS for a server to signal that it has started
    // correctly.
    Status start_and_wait_for_server();

private:
    // Lock used to ensure that there are no missed notifications between starting the
    // supervision thread and calling _signal_cond.timed_wait. Also used to ensure
    // thread-safe access to members of _thrift_server
    boost::mutex _signal_lock;

    // Condition variable that is notified by the supervision thread once either
    // a) all is well or b) an error occurred.
    boost::condition_variable _signal_cond;

    // The ThriftServer under management. This class is a friend of ThriftServer, and
    // reaches in to change member variables at will.
    ThriftServer* _thrift_server;

    // Guards against spurious condition variable wakeups
    bool _signal_fired;

    // The time, in milliseconds, to wait for a server to come up
    static const int TIMEOUT_MS = 2500;

    // Called in a separate thread; wraps TNonBlockingServer::serve in an exception handler
    void supervise();
};

Status ThriftServer::ThriftServerEventProcessor::start_and_wait_for_server() {
    // Locking here protects against missed notifications if Supervise executes quickly
    boost::unique_lock<boost::mutex> lock(_signal_lock);
    _thrift_server->_started = false;

    _thrift_server->_server_thread.reset(
            new boost::thread(&ThriftServer::ThriftServerEventProcessor::supervise, this));

    boost::system_time deadline =
            boost::get_system_time() + boost::posix_time::milliseconds(TIMEOUT_MS);

    // Loop protects against spurious wakeup. Locks provide necessary fences to ensure
    // visibility.
    while (!_signal_fired) {
        // Yields lock and allows supervision thread to continue and signal
        if (!_signal_cond.timed_wait(lock, deadline)) {
            std::stringstream ss;
            ss << "ThriftServer '" << _thrift_server->_name
               << "' (on port: " << _thrift_server->_port << ") did not start within " << TIMEOUT_MS
               << "ms";
            LOG(ERROR) << ss.str();
            return Status::InternalError(ss.str());
        }
    }

    // _started == true only if preServe was called. May be false if there was an exception
    // after preServe that was caught by Supervise, causing it to reset the error condition.
    if (_thrift_server->_started == false) {
        std::stringstream ss;
        ss << "ThriftServer '" << _thrift_server->_name << "' (on port: " << _thrift_server->_port
           << ") did not start correctly ";
        LOG(ERROR) << ss.str();
        return Status::InternalError(ss.str());
    }

    return Status::OK();
}

void ThriftServer::ThriftServerEventProcessor::supervise() {
    DCHECK(_thrift_server->_server.get() != NULL);

    try {
        _thrift_server->_server->serve();
    } catch (apache::thrift::TException& e) {
        LOG(ERROR) << "ThriftServer '" << _thrift_server->_name
                   << "' (on port: " << _thrift_server->_port
                   << ") exited due to TException: " << e.what();
    }

    {
        // _signal_lock ensures mutual exclusion of access to _thrift_server
        boost::lock_guard<boost::mutex> lock(_signal_lock);
        _thrift_server->_started = false;

        // There may not be anyone waiting on this signal (if the
        // exception occurs after startup). That's not a problem, this is
        // just to avoid waiting for the timeout in case of a bind
        // failure, for example.
        _signal_fired = true;
    }

    _signal_cond.notify_all();
}

void ThriftServer::ThriftServerEventProcessor::preServe() {
    // Acquire the signal lock to ensure that StartAndWaitForServer is
    // waiting on _signal_cond when we notify.
    boost::lock_guard<boost::mutex> lock(_signal_lock);
    _signal_fired = true;

    // This is the (only) success path - if this is not reached within TIMEOUT_MS,
    // StartAndWaitForServer will indicate failure.
    _thrift_server->_started = true;

    // Should only be one thread waiting on _signal_cond, but wake all just in case.
    _signal_cond.notify_all();
}

// This thread-local variable contains the current session key for whichever
// thrift server is currently serving a request on the current thread.
__thread ThriftServer::SessionKey* _session_key;

ThriftServer::SessionKey* ThriftServer::get_thread_session_key() {
    return _session_key;
}

void* ThriftServer::ThriftServerEventProcessor::createContext(
        boost::shared_ptr<apache::thrift::protocol::TProtocol> input,
        boost::shared_ptr<apache::thrift::protocol::TProtocol> output) {
    std::stringstream ss;

    apache::thrift::transport::TSocket* socket = NULL;
    apache::thrift::transport::TTransport* transport = input->getTransport().get();
    {
        switch (_thrift_server->_server_type) {
        case NON_BLOCKING:
            socket = static_cast<apache::thrift::transport::TSocket*>(
                    static_cast<apache::thrift::transport::TFramedTransport*>(transport)
                            ->getUnderlyingTransport()
                            .get());
            break;

        case THREAD_POOL:
        case THREADED:
            socket = static_cast<apache::thrift::transport::TSocket*>(
                    static_cast<apache::thrift::transport::TBufferedTransport*>(transport)
                            ->getUnderlyingTransport()
                            .get());
            break;

        default:
            DCHECK(false) << "Unexpected thrift server type";
        }
    }

    ss << socket->getPeerAddress() << ":" << socket->getPeerPort();

    {
        boost::lock_guard<boost::mutex> _l(_thrift_server->_session_keys_lock);

        boost::shared_ptr<SessionKey> key_ptr(new std::string(ss.str()));

        _session_key = key_ptr.get();
        _thrift_server->_session_keys[key_ptr.get()] = key_ptr;
    }

    if (_thrift_server->_session_handler != NULL) {
        _thrift_server->_session_handler->session_start(*_session_key);
    }

    _thrift_server->thrift_connections_total->increment(1L);
    _thrift_server->thrift_current_connections->increment(1L);

    // Store the _session_key in the per-client context to avoid recomputing
    // it. If only this were accessible from RPC method calls, we wouldn't have to
    // mess around with thread locals.
    return (void*)_session_key;
}

void ThriftServer::ThriftServerEventProcessor::processContext(
        void* context, boost::shared_ptr<apache::thrift::transport::TTransport> transport) {
    _session_key = reinterpret_cast<SessionKey*>(context);
}

void ThriftServer::ThriftServerEventProcessor::deleteContext(
        void* serverContext, boost::shared_ptr<apache::thrift::protocol::TProtocol> input,
        boost::shared_ptr<apache::thrift::protocol::TProtocol> output) {
    _session_key = (SessionKey*)serverContext;

    if (_thrift_server->_session_handler != NULL) {
        _thrift_server->_session_handler->session_end(*_session_key);
    }

    {
        boost::lock_guard<boost::mutex> _l(_thrift_server->_session_keys_lock);
        _thrift_server->_session_keys.erase(_session_key);
    }

    _thrift_server->thrift_current_connections->increment(-1L);
}

ThriftServer::ThriftServer(const std::string& name,
                           const boost::shared_ptr<apache::thrift::TProcessor>& processor, int port,
                           int num_worker_threads, ServerType server_type)
        : _started(false),
          _port(port),
          _num_worker_threads(num_worker_threads),
          _server_type(server_type),
          _name(name),
          _server_thread(NULL),
          _server(NULL),
          _processor(processor),
          _session_handler(NULL) {
    _thrift_server_metric_entity = DorisMetrics::instance()->metric_registry()->register_entity(
            std::string("thrift_server.") + name, {{"name", name}});
    INT_GAUGE_METRIC_REGISTER(_thrift_server_metric_entity, thrift_current_connections);
    INT_COUNTER_METRIC_REGISTER(_thrift_server_metric_entity, thrift_connections_total);
}

Status ThriftServer::start() {
    DCHECK(!_started);
    boost::shared_ptr<apache::thrift::protocol::TProtocolFactory> protocol_factory(
            new apache::thrift::protocol::TBinaryProtocolFactory());
    boost::shared_ptr<apache::thrift::concurrency::ThreadManager> thread_mgr;
    boost::shared_ptr<apache::thrift::concurrency::ThreadFactory> thread_factory(
            new apache::thrift::concurrency::PosixThreadFactory());
    boost::shared_ptr<apache::thrift::transport::TServerTransport> fe_server_transport;
    boost::shared_ptr<apache::thrift::transport::TTransportFactory> transport_factory;

    if (_server_type != THREADED) {
        thread_mgr = apache::thrift::concurrency::ThreadManager::newSimpleThreadManager(
                _num_worker_threads);
        thread_mgr->threadFactory(thread_factory);
        thread_mgr->start();
    }

    // Note - if you change the transport types here, you must check that the
    // logic in createContext is still accurate.
    apache::thrift::transport::TServerSocket* server_socket = NULL;

    switch (_server_type) {
    case NON_BLOCKING:
        if (transport_factory.get() == NULL) {
            transport_factory.reset(new apache::thrift::transport::TTransportFactory());
        }

        _server.reset(new apache::thrift::server::TNonblockingServer(
                _processor, transport_factory, transport_factory, protocol_factory,
                protocol_factory, _port, thread_mgr));
        break;

    case THREAD_POOL:
        fe_server_transport.reset(new apache::thrift::transport::TServerSocket(_port));

        if (transport_factory.get() == NULL) {
            transport_factory.reset(new apache::thrift::transport::TBufferedTransportFactory());
        }

        _server.reset(new apache::thrift::server::TThreadPoolServer(
                _processor, fe_server_transport, transport_factory, protocol_factory, thread_mgr));
        break;

    case THREADED:
        server_socket = new apache::thrift::transport::TServerSocket(_port);
        //      server_socket->setAcceptTimeout(500);
        fe_server_transport.reset(server_socket);

        if (transport_factory.get() == NULL) {
            transport_factory.reset(new apache::thrift::transport::TBufferedTransportFactory());
        }

        _server.reset(new apache::thrift::server::TThreadedServer(
                _processor, fe_server_transport, transport_factory, protocol_factory,
                thread_factory));
        break;

    default:
        std::stringstream error_msg;
        error_msg << "Unsupported server type: " << _server_type;
        LOG(ERROR) << error_msg.str();
        return Status::InternalError(error_msg.str());
    }

    boost::shared_ptr<ThriftServer::ThriftServerEventProcessor> event_processor(
            new ThriftServer::ThriftServerEventProcessor(this));
    _server->setServerEventHandler(event_processor);

    RETURN_IF_ERROR(event_processor->start_and_wait_for_server());

    LOG(INFO) << "ThriftServer '" << _name << "' started on port: " << _port;

    DCHECK(_started);
    return Status::OK();
}

void ThriftServer::stop() {
    _server->stop();
}

void ThriftServer::join() {
    DCHECK(_server_thread != NULL);
    DCHECK(_started);
    _server_thread->join();
}

void ThriftServer::stop_for_testing() {
    DCHECK(_server_thread != NULL);
    DCHECK(_server);
    DCHECK_EQ(_server_type, THREADED);
    _server->stop();

    if (_started) {
        join();
    }
}
} // namespace doris
