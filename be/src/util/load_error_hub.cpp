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

#include "util/load_error_hub.h"

#include <thrift/protocol/TDebugProtocol.h>

#include "gen_cpp/PaloInternalService_types.h"
#include "util/broker_load_error_hub.h"
#include "util/mysql_load_error_hub.h"
#include "util/null_load_error_hub.h"

namespace doris {

Status LoadErrorHub::create_hub(ExecEnv* env, const TLoadErrorHubInfo* t_hub_info,
                                const std::string& error_log_file_name,
                                std::unique_ptr<LoadErrorHub>* hub) {
    LoadErrorHub* tmp_hub = nullptr;

    if (t_hub_info == nullptr) {
        tmp_hub = new NullLoadErrorHub();
        tmp_hub->prepare();
        hub->reset(tmp_hub);
        return Status::OK();
    }

    VLOG_ROW << "create_hub: " << apache::thrift::ThriftDebugString(*t_hub_info).c_str();

    switch (t_hub_info->type) {
    case TErrorHubType::MYSQL:
#ifdef DORIS_WITH_MYSQL
        tmp_hub = new MysqlLoadErrorHub(t_hub_info->mysql_info);
        tmp_hub->prepare();
        hub->reset(tmp_hub);
        break;
#else
        return Status::InternalError(
                "Don't support MySQL table, you should rebuild Doris with WITH_MYSQL option ON");
#endif
    case TErrorHubType::BROKER: {
        // the origin file name may contains __shard_0/xxx
        // replace the '/' with '_'
        std::string copied_name(error_log_file_name);
        std::replace(copied_name.begin(), copied_name.end(), '/', '_');
        tmp_hub = new BrokerLoadErrorHub(env, t_hub_info->broker_info, copied_name);
        tmp_hub->prepare();
        hub->reset(tmp_hub);
        break;
    }
    case TErrorHubType::NULL_TYPE:
        tmp_hub = new NullLoadErrorHub();
        tmp_hub->prepare();
        hub->reset(tmp_hub);
        break;
    default:
        std::stringstream err;
        err << "Unknown hub type." << t_hub_info->type;
        return Status::InternalError(err.str());
    }

    return Status::OK();
}

} // end namespace doris
