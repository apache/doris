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

#include <arrow/type_fwd.h>
#include <gen_cpp/ExternalTableSchema_types.h>

#include <memory>
#include <string>

#include "common/status.h"

namespace doris {

class Block;

namespace paimon {

/// Builds the exact Arrow schema consumed by Arrow's Parquet writer for a
/// Paimon data file. In particular this preserves Paimon field ids, synthetic
/// collection ids, nullability, timestamp semantics and short-decimal layout.
class ArrowSchemaUtil {
public:
    static Status convert(const schema::external::TSchema& schema, const Block& block,
                          const std::string& timezone,
                          std::shared_ptr<arrow::Schema>* arrow_schema);
};

} // namespace paimon
} // namespace doris
