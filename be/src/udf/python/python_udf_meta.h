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

#include <sys/types.h>

#include "arrow/buffer.h"
#include "arrow/flight/client.h"
#include "arrow/flight/server.h"
#include "common/status.h"
#include "util/arrow/row_batch.h"
#include "vec/data_types/data_type.h"

namespace doris {

enum class PythonUDFLoadType : uint8_t { INLINE = 0, MODULE = 1, UNKNOWN = 2 };

struct PythonUDFMeta {
    int64_t _id;
    std::string _name;
    std::string _symbol;
    std::string _location;
    std::string _checksum;
    std::string _runtime_version;
    std::string _inline_code;
    bool _always_nullable;
    vectorized::DataTypes _input_types;
    vectorized::DataTypePtr _return_type;
    PythonUDFLoadType _type;

    static Status convert_types_to_schema(const vectorized::DataTypes& types,
                                          const std::string& timezone,
                                          std::shared_ptr<arrow::Schema>* schema);

    static Status serialize_arrow_schema(const std::shared_ptr<arrow::Schema>& schema,
                                         std::shared_ptr<arrow::Buffer>* out);

    Status serialize_to_json(std::string* json_str) const;

    std::string to_string() const;

    Status check() const;
};

} // namespace doris