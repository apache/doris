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

#include <mutex>
#include <string>
#include <vector>

#include "cpp/container_credentials_test_util.h"
#include "service/http/ev_http_server.h"
#include "service/http/http_channel.h"
#include "service/http/http_handler.h"
#include "service/http/http_headers.h"
#include "service/http/http_method.h"
#include "service/http/http_request.h"

// Scaffolding for tests of the container credentials providers - the ECS task-role and EKS Pod
// Identity endpoints. A credible test needs three awkward pieces at once: a real HTTP endpoint on an
// address the AWS SDK is willing to talk to, a token file that can be rewritten mid-test, and the
// four AWS_CONTAINER_* environment variables put back however they were found.
//
// Only the first of those lives here, because it is the only one the cloud test tree cannot use: it
// needs EvHttpServer, which is BE-only. The other two - ContainerCredentialsEnvGuard and
// as_valid_http_provider - come from cpp/container_credentials_test_util.h, included above and
// re-exported to anything that includes this header, so BE tests keep getting all three from one
// include.
namespace doris {

// A mock of the credentials endpoint that ECS and the EKS Pod Identity agent expose.
class ContainerCredentialsHandler : public HttpHandler {
public:
    void handle(HttpRequest* req) override {
        {
            std::lock_guard<std::mutex> lock(_mutex);
            _auth_headers.push_back(req->header(HttpHeaders::AUTHORIZATION));
        }

        req->add_output_header(HttpHeaders::CONTENT_TYPE, "application/json");
        // Expiration in the past keeps ExpiresSoon() true, so the provider
        // re-reads the token file on the next call instead of serving its cache.
        HttpChannel::send_reply(req,
                                R"({"AccessKeyId":"AKIDTEST","SecretAccessKey":"SECRETTEST",)"
                                R"("Token":"SESSIONTEST","Expiration":"1970-01-01T00:00:00Z"})");
    }

    std::vector<std::string> auth_headers() {
        std::lock_guard<std::mutex> lock(_mutex);
        return _auth_headers;
    }

private:
    std::mutex _mutex;
    std::vector<std::string> _auth_headers;
};

// Starts the mock endpoint on an OS-assigned loopback port and hands out its URL.
class ContainerCredentialsEndpoint {
public:
    bool start() {
        if (!_server.register_handler(GET, "/creds", &_handler)) {
            return false;
        }
        _server.start();
        if (_server.get_real_port() == 0) {
            return false;
        }
        _url = "http://127.0.0.1:" + std::to_string(_server.get_real_port()) + "/creds";
        return true;
    }

    const std::string& url() const { return _url; }

    std::vector<std::string> auth_headers() { return _handler.auth_headers(); }

private:
    // _handler is declared before _server so that it is destroyed after it: ~EvHttpServer() stops
    // the server, and stop() only returns once every worker thread has left its event loop, so no
    // request can still be inside _handler by then.
    ContainerCredentialsHandler _handler;
    EvHttpServer _server {0};
    std::string _url;
};

} // namespace doris
