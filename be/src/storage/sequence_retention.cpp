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

#include "storage/sequence_retention.h"

#include "core/assert_cast.h"
#include "core/column/column_vector.h"
#include "storage/row_ttl.h"
#include "storage/utils.h"

namespace doris {

Status apply_sequence_retention_delete_filter(const Block& block, int64_t prune_version,
                                              RowVisibilityFilter* filter) {
    if (prune_version < 0) {
        return Status::OK();
    }
    const int delete_position = block.get_position_by_name(DELETE_SIGN);
    const int version_position = block.get_position_by_name(VERSION_COL);
    if (delete_position < 0 || version_position < 0) {
        return Status::InternalError(
                "sequence retention compaction requires delete sign and VERSION in its key group");
    }
    DCHECK_EQ(filter->selection.size(), block.rows());
    // Storage readers materialize the hidden columns, including singleton rowset versions,
    // into full non-null integer columns before post-merge filtering.
    const auto& deletes =
            assert_cast<const ColumnInt8&>(*block.get_by_position(delete_position).column)
                    .get_data();
    const auto& versions =
            assert_cast<const ColumnInt64&>(*block.get_by_position(version_position).column)
                    .get_data();
    for (size_t row = 0; row < block.rows(); ++row) {
        if (filter->selection[row] && deletes[row] != 0) {
            if (versions[row] <= 0) {
                return Status::InternalError(
                        "sequence retention cannot prune a published delete sign with original "
                        "version {}",
                        versions[row]);
            }
            if (versions[row] <= prune_version) {
                filter->selection[row] = 0;
                ++filter->rows_deleted;
            }
        }
    }
    return Status::OK();
}

int64_t sequence_retention_prune_version(int64_t published_version, int64_t gap) {
    DCHECK_GT(gap, 0);
    // Subtract twice with saturation because 2*gap can exceed BIGINT.
    const auto cutoff = published_version > gap ? published_version - gap : 0;
    return cutoff > gap ? cutoff - gap : 0;
}

} // namespace doris
