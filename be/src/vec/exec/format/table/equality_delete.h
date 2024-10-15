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

#include "exprs/create_predicate_function.h"
#include "util/runtime_profile.h"
#include "vec/core/block.h"

namespace doris::vectorized {

/**
 * Support Iceberg equality delete.
 * If there's only one delete column in delete file, use `SimpleEqualityDelete`,
 * which uses optimized `HybridSetBase` to build the hash set.
 * If there are more delete columns in delete file, use `MultiEqualityDelete`,
 * which generates a hash column from all delete columns, and only compare the values
 * when the hash values are the same.
 */
class EqualityDeleteBase {
protected:
    RuntimeProfile::Counter* num_delete_rows;
    RuntimeProfile::Counter* build_set_time;
    RuntimeProfile::Counter* equality_delete_time;

    Block* _delete_block;

    virtual Status _build_set() = 0;

public:
    EqualityDeleteBase(Block* delete_block) : _delete_block(delete_block) {}
    virtual ~EqualityDeleteBase() = default;

    Status init(RuntimeProfile* profile) {
        static const char* delete_profile = "EqualityDelete";
        ADD_TIMER_WITH_LEVEL(profile, delete_profile, 1);
        num_delete_rows = ADD_CHILD_COUNTER_WITH_LEVEL(profile, "NumRowsInDeleteFile", TUnit::UNIT,
                                                       delete_profile, 1);
        build_set_time = ADD_CHILD_TIMER_WITH_LEVEL(profile, "BuildHashSetTime", delete_profile, 1);
        equality_delete_time =
                ADD_CHILD_TIMER_WITH_LEVEL(profile, "EqualityDeleteFilterTime", delete_profile, 1);
        SCOPED_TIMER(build_set_time);
        return _build_set();
    }

    virtual Status filter_data_block(Block* data_block) = 0;

    static std::unique_ptr<EqualityDeleteBase> get_delete_impl(Block* delete_block);
};

class SimpleEqualityDelete : public EqualityDeleteBase {
protected:
    std::shared_ptr<HybridSetBase> _hybrid_set;
    std::string _delete_column_name;
    PrimitiveType _delete_column_type;
    std::unique_ptr<IColumn::Filter> _filter;

    Status _build_set() override;

public:
    SimpleEqualityDelete(Block* delete_block) : EqualityDeleteBase(delete_block) {}

    Status filter_data_block(Block* data_block) override;
};

/**
 * `MultiEqualityDelete` will generate the hash column for delete block and data block.
 */
class MultiEqualityDelete : public EqualityDeleteBase {
protected:
    // hash column for delete block
    std::vector<uint64_t> _delete_hashes;
    // hash column for data block
    std::vector<uint64_t> _data_hashes;
    // hash code => row index
    // if hash values are equal, then compare the real values
    // the row index records the row number of the delete row in delete block
    std::multimap<uint64_t, size_t> _delete_hash_map;
    // the delete column indexes in data block
    std::vector<size_t> _data_column_index;
    std::unique_ptr<IColumn::Filter> _filter;

    Status _build_set() override;

    bool _equal(Block* data_block, size_t data_row_index, size_t delete_row_index);

public:
    MultiEqualityDelete(Block* delete_block) : EqualityDeleteBase(delete_block) {}

    Status filter_data_block(Block* data_block) override;
};

} // namespace doris::vectorized
