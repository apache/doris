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

#include "format/arrow/arrow_block_convertor.h"

namespace doris {

class ParquetArrowBlockConvertor : public ArrowBlockConvertor {
public:
    ParquetArrowBlockConvertor(DataTypes types, std::vector<std::string> names,
                               std::string timezone_name, const cctz::time_zone& timezone,
                               bool enable_int96_timestamps)
            : ArrowBlockConvertor(nullptr, timezone),
              _types(std::move(types)),
              _names(std::move(names)),
              _timezone_name(std::move(timezone_name)),
              _enable_int96_timestamps(enable_int96_timestamps) {}
    Status init() override;

protected:
    Status write_column(const DataTypePtr& type, const DataTypeSerDe& serde, const IColumn& column,
                        const NullMap* null_map, const std::shared_ptr<arrow::Field>& field,
                        arrow::ArrayBuilder* builder, int64_t start, int64_t end,
                        const cctz::time_zone& timezone) const override;

private:
    DataTypes _types;
    std::vector<std::string> _names;
    std::string _timezone_name;
    bool _enable_int96_timestamps;
};

} // namespace doris
