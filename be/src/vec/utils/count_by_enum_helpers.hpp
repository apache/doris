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

#include <rapidjson/document.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/stringbuffer.h>

#include <boost/dynamic_bitset.hpp>

#include "vec/data_types/data_type_decimal.h"
#include "vec/io/io_helper.h"

namespace doris::vectorized {

struct CountByEnumData {
    std::unordered_map<std::string, uint64_t> cbe;
    uint64_t not_null;
    uint64_t null;
    uint64_t all;
};

void build_json_from_vec(rapidjson::StringBuffer& buffer, const std::vector<CountByEnumData>& data_vec) {
    rapidjson::Document doc;
    doc.SetArray();
    rapidjson::Document::AllocatorType& allocator = doc.GetAllocator();

    int vec_size_number = data_vec.size();
    for (int idx = 0; idx < vec_size_number; ++idx) {
        rapidjson::Value obj(rapidjson::kObjectType);

        rapidjson::Value obj_cbe(rapidjson::kObjectType);
        std::unordered_map<std::string, uint64_t> unordered_map = data_vec[idx].cbe;
        for (auto it : unordered_map) {
            rapidjson::Value key_cbe(it.first.c_str(), allocator);
            rapidjson::Value value_cbe(it.second);
            obj_cbe.AddMember(key_cbe, value_cbe, allocator);
        }
        obj.AddMember("cbe", obj_cbe, allocator);
        obj.AddMember("notnull", data_vec[idx].not_null, allocator);
        obj.AddMember("null", data_vec[idx].null, allocator);
        obj.AddMember("all", data_vec[idx].all, allocator);

        doc.PushBack(obj, allocator);
    }

    rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
    doc.Accept(writer);
}

} // namespace  doris::vectorized