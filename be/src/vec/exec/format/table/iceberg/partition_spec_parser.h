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

//#include <rapidjson/document.h>
//#include <rapidjson/error/en.h>
//
//#include <unordered_set>
//
//#include "common/exception.h"
//#include "vec/exec/format/table/iceberg/schema.h"
#include <rapidjson/document.h>

#include "vec/exec/format/table/iceberg/unbound_partition_spec.h"

namespace doris {
namespace iceberg {

class PartitionSpec;
class Schema;

class PartitionSpecParser {
private:
    static const char* SPEC_ID;
    static const char* FIELDS;
    static const char* SOURCE_ID;
    static const char* FIELD_ID;
    static const char* TRANSFORM;
    static const char* NAME;

public:
    static std::unique_ptr<PartitionSpec> from_json(const std::shared_ptr<Schema>& schema,
                                                    const std::string& json);

private:
    static void _build_from_json_fields(UnboundPartitionSpec::Builder& builder,
                                        const rapidjson::Value& value);
};

} // namespace iceberg
} // namespace doris
