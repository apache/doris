// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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

namespace palo {

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

}
