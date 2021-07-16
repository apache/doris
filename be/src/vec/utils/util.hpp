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
#include <thrift/protocol/TJSONProtocol.h>

#include <boost/shared_ptr.hpp>

#include "runtime/descriptors.h"
#include "vec/core/block.h"
#include "vec/columns/column_nullable.h"

namespace doris::vectorized {
class VectorizedUtils {
public:
    static Block create_empty_columnswithtypename(const RowDescriptor& row_desc) {
        // Block block;
        return create_columns_with_type_and_name(row_desc);
    }

    static ColumnsWithTypeAndName create_columns_with_type_and_name(const RowDescriptor& row_desc) {
        ColumnsWithTypeAndName columns_with_type_and_name;
        for (const auto& tuple_desc : row_desc.tuple_descriptors()) {
            for (const auto& slot_desc : tuple_desc->slots()) {
                columns_with_type_and_name.emplace_back(nullptr, slot_desc->get_data_type_ptr(),
                                                        slot_desc->col_name());
            }
        }
        return columns_with_type_and_name;
    }

    static void update_null_map(NullMap& dst, const NullMap& src) {
        size_t size = dst.size();
        for (size_t i = 0; i < size; ++i)
            if (src[i]) dst[i] = 1;
    }
};
} // namespace doris::vectorized

namespace apache::thrift {
template <typename ThriftStruct>
ThriftStruct from_json_string(const std::string& json_val) {
    using namespace apache::thrift::transport;
    using namespace apache::thrift::protocol;
    ThriftStruct ts;
    TMemoryBuffer* buffer =
            new TMemoryBuffer((uint8_t*)json_val.c_str(), (uint32_t)json_val.size());
    boost::shared_ptr<TTransport> trans(buffer);
    TJSONProtocol protocol(trans);
    ts.read(&protocol);
    return ts;
}
} // namespace apache::thrift
