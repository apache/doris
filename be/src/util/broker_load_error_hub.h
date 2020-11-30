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

#ifndef DORIS_BE_SRC_UTIL_BROKER_LOAD_ERROR_HUB_H
#define DORIS_BE_SRC_UTIL_BROKER_LOAD_ERROR_HUB_H

#include <mutex>
#include <queue>
#include <sstream>
#include <string>

#include "gen_cpp/PaloInternalService_types.h"
#include "util/load_error_hub.h"

namespace doris {

class BrokerWriter;
class ExecEnv;

// Broker load error hub will write load error info to the specified
// remote storage via broker.
// We should only open this error hub if there are error line.
// Because open the writer via broker may cost several seconds.
class BrokerLoadErrorHub : public LoadErrorHub {
public:
    struct BrokerInfo {
        std::vector<TNetworkAddress> addrs;
        // path should be like:
        // xxx://yyy/file_name
        std::string path;
        std::map<std::string, std::string> props;

        BrokerInfo(const TBrokerErrorHubInfo& t_info, const std::string& error_log_file_name)
                : props(t_info.prop) {
            path = t_info.path + "/" + error_log_file_name;
            addrs.push_back(t_info.broker_addr);
        }
    };

    BrokerLoadErrorHub(ExecEnv* env, const TBrokerErrorHubInfo& info,
                       const std::string& error_log_file_name);

    virtual ~BrokerLoadErrorHub();

    virtual Status prepare();

    virtual Status export_error(const ErrorMsg& error_msg);

    virtual Status close();

    virtual std::string debug_string() const;

private:
    Status write_to_broker();

    ExecEnv* _env;
    BrokerInfo _info;

    // the number in a write batch.
    static const int32_t EXPORTER_THRESHOLD = 20;

    BrokerWriter* _broker_writer;
    std::mutex _mtx;
    std::queue<ErrorMsg> _error_msgs;
    int32_t _total_error_num = 0;

}; // end class BrokerLoadErrorHub

} // end namespace doris

#endif // DORIS_BE_SRC_UTIL_BROKER_LOAD_ERROR_HUB_H
