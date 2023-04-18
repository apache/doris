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

#include "data_type_serde.h"
namespace doris {

namespace vectorized {

class DataTypeMapSerDe : public DataTypeSerDe {
public:
    DataTypeMapSerDe(const DataTypeSerDeSPtr& _key_serde, const DataTypeSerDeSPtr& _value_serde)
            : key_serde(_key_serde), value_serde(_value_serde) {}

    Status write_column_to_pb(const IColumn& column, PValues& result, int start,
                              int end) const override {
        LOG(FATAL) << "Not support write map column to pb";
    }
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override {
        LOG(FATAL) << "Not support read from pb to map";
    }

private:
    DataTypeSerDeSPtr key_serde;
    DataTypeSerDeSPtr value_serde;
};
} // namespace vectorized
} // namespace doris
