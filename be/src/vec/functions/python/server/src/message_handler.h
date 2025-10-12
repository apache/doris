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

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include "../../common/interaction_channel.h"
#include "../../common/interaction_protocol.h"
#include "server_config.h"

extern InteractionChannel channel;

/**
 * Send the response to client.
 */
inline void send_response(const doris::pyudf::ServerResponse& response) {
    if (!channel.send(&response.status_code, sizeof(size_t))) {
        throw std::runtime_error("failed to send status_code");
    }
    if (!channel.send(&response.payload_length, sizeof(size_t))) {
        throw std::runtime_error("failed to send payload_length");
    }
    if (response.payload_length > 0) {
        if (!channel.send(response.payload.c_str(), response.payload_length)) {
            throw std::runtime_error("failed to send payload");
        }
    }
}

/**
 * Get the request from client.
 */
inline void get_request(doris::pyudf::ClientRequest& request) {
    if (!channel.recv(&request.command_type, sizeof(size_t))) {
        throw std::runtime_error("failed to recv command_type");
    }
    if (!channel.recv(&request.payload_length, sizeof(size_t))) {
        throw std::runtime_error("failed to recv payload_length");
    }
    if (request.payload_length <= 0) {
        return;
    }
    const std::string buffer(request.payload_length, '\0');
    request.payload = buffer;
    if (!channel.recv(request.payload.data(), request.payload_length)) {
        throw std::runtime_error("failed to recv payload");
    }
}

/**
 * Send the python error response to client.
 */
void send_python_error_response();

/**
 * Send the custom error response to client.
 */
void send_custom_error_response(const std::string& error_message);

/**
 * Send the timeout response to client.
 */
inline void send_timeout_response() {
    const doris::pyudf::ServerResponse response(doris::pyudf::TIMEOUT);
    send_response(response);
}

/**
 * {@link ExecutionMonitor} monitors the execution of Python UDFs and triggers a timeout if the
 * execution time exceeds a specified limit.
 */
class ExecutionMonitor {
public:
    ExecutionMonitor() = default;

    /**
     * Starts the execution monitor thread.
     */
    void start() { monitor_thread = std::thread(&ExecutionMonitor::monitor, this); }

    /**
     * The main loop of the execution monitor thread that checks for execution.
     */
    void monitor() {
        while (true) {
            std::this_thread::sleep_for(std::chrono::seconds(MONITOR_INTERVAL_SECONDS));
            std::unique_lock lock(mtx);
            if (_execution_finished) {
                continue;
            }
            const int current_round_snapshot = current_round;
            cv.wait_for(lock, std::chrono::seconds(doris::pyudf::MAX_EXECUTE_TIME_SECONDS),
                        [this] { return _execution_finished; });
            // Ensure _execution_finished hasn't been changed by main thread by comparing the round number.
            if (!_execution_finished && current_round_snapshot == current_round) {
                send_timeout_response();
            }
        }
    }

    /**
     * Marks the start of a new UDF execution.
     */
    void start_execution() {
        std::lock_guard lock(mtx);
        _execution_finished = false;
        current_round++;
    }

    /**
     * Marks the end of a UDF execution.
     */
    void end_execution() {
        std::lock_guard lock(mtx);
        _execution_finished = true;
        cv.notify_one();
    }

private:
    std::thread monitor_thread;
    std::mutex mtx;
    std::condition_variable cv;
    bool _execution_finished = false;
    int current_round = 0;
};
