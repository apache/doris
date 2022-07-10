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

#include <cstdlib>
#include <sstream>
#include <string>

namespace doris {

struct S3Conf {
    std::string ak;
    std::string sk;
    std::string endpoint;
    std::string region;
    std::string bucket;
    std::string prefix;

    void init_from_env();

    std::string to_string() const;
};

inline void S3Conf::init_from_env() {
    char* var = std::getenv("S3_AK");
    ak = var ? var : "";
    var = std::getenv("S3_SK");
    sk = var ? var : "";
    var = std::getenv("S3_ENDPOINT");
    endpoint = var ? var : "";
    var = std::getenv("S3_REGION");
    region = var ? var : "";
    var = std::getenv("S3_BUCKET");
    bucket = var ? var : "";
    var = std::getenv("S3_PREFIX");
    prefix = var ? var : "";
}

inline std::string S3Conf::to_string() const {
    std::stringstream ss;
    ss << "ak: " << ak << ", sk: " << sk << ", endpoint: " << endpoint << ", region: " << region
       << ", bucket: " << bucket << ", prefix: " << prefix;
    return ss.str();
}

} // namespace doris
