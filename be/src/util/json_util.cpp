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

#include "util/json_util.h"

#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

namespace doris {

std::string to_json(const Status& status) {
    rapidjson::StringBuffer s;
    rapidjson::PrettyWriter<rapidjson::StringBuffer> writer(s);

    writer.StartObject();
    // status
    writer.Key("status");
    if (status.ok()) {
        writer.String("Success");
    } else {
        writer.String("Fail");
    }
    // msg
    writer.Key("msg");
    if (status.ok()) {
        writer.String("OK");
    } else {
        writer.String(status.get_error_msg().c_str());
    }
    writer.EndObject();
    return s.GetString();
}

} // namespace doris
