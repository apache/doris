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
#include <memory>
#include <string>

#include "common/status.h"
#include "vec/columns/column.h"

namespace doris {

namespace vectorized {
// Deserialize means read from different file format or memory format,
// for example read from arrow, read from parquet.
// Serialize means write the column cell or the total column into another
// memory format or file format.
// Every data type should implement this interface.
// In the past, there are many switch cases in the code and we do not know
// how many cases or files we has to modify when we add a new type. And also
// it is very difficult to add a new read file format or write file format because
// the developer does not know how many datatypes has to deal.

class DataTypeSerDe {
public:
    DataTypeSerDe();
    virtual ~DataTypeSerDe();

    // Protobuf serializer and deserializer
    virtual Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                                      int end) const = 0;

    virtual Status read_column_from_pb(IColumn& column, const PValues& arg) const = 0;

    // JSONB serializer and deserializer

    // MySQL serializer and deserializer

    // Thrift serializer and deserializer

    // ORC serializer and deserializer

    // CSV serializer and deserializer

    // JSON serializer and deserializer

    // Arrow serializer and deserializer
};

using DataTypeSerDeSPtr = std::shared_ptr<DataTypeSerDe>;
using DataTypeSerDeSPtrs = std::vector<DataTypeSerDeSPtr>;

} // namespace vectorized
} // namespace doris