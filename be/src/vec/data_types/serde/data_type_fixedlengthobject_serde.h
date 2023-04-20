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

#include <glog/logging.h>

#include <ostream>

#include "common/status.h"
#include "data_type_serde.h"

namespace doris {
class PValues;

namespace vectorized {
class IColumn;

class DataTypeFixedLengthObjectSerDe : public DataTypeSerDe {
public:
    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override {
        LOG(FATAL) << "Not support write FixedLengthObject column to pb";
    }
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override {
        LOG(FATAL) << "Not support read from pb to FixedLengthObject";
    };
};
} // namespace vectorized
} // namespace doris
