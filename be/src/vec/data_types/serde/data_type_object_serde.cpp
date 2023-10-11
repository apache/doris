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

#include "data_type_object_serde.h"

#include "vec/columns/column_complex.h"
namespace doris {

namespace vectorized {
Status DataTypeObjectSerDe::write_column_to_orc(const IColumn& column, const NullMap* null_map,
                                                orc::ColumnVectorBatch* orc_col_batch, int start,
                                                int end,
                                                std::vector<StringRef>& buffer_list) const {
    auto& col_data = assert_cast<const ColumnBitmap&>(column);
    orc::StringVectorBatch* cur_batch = dynamic_cast<orc::StringVectorBatch*>(orc_col_batch);

    for (size_t row_id = start; row_id < end; row_id++) {
        if (cur_batch->notNull[row_id] == 1) {
            const auto& ele = col_data.get_data_at(row_id);
            cur_batch->data[row_id] = const_cast<char*>(ele.data);
            cur_batch->length[row_id] = ele.size;
        }
    }

    cur_batch->numElements = end - start;
    return Status::OK();
}
} // namespace vectorized
} // namespace doris