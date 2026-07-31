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

#include "common/status.h"
#include "core/block/block.h"
#include "storage/olap_define.h"
#include "storage/olap_utils.h"
#include "storage/partial_update_info.h"
#include "storage/tablet/tablet_fwd.h"

namespace doris {
struct RowsetWriterContext;
class KeyCoder;
struct MowContext;

namespace segment_v2 {

struct HistoricalRowRetrieverContext {
    BaseTabletSPtr tablet;
    TabletSchemaSPtr tablet_schema;
    RowsetWriterContext* rowset_writer_ctx = nullptr;
    std::shared_ptr<PartialUpdateInfo> partial_update_info;
    bool is_transient_rowset_writer = false;
    DataWriteType write_type = DataWriteType::TYPE_DEFAULT;
};

class HistoricalRowRetriever {
public:
    HistoricalRowRetriever() = default;
    virtual ~HistoricalRowRetriever() = default;

    virtual Status init(const HistoricalRowRetrieverContext& rowset_writer_context) = 0;

    virtual Status retrieve_historical_row(const Int8* delete_sign_column_data, size_t row_pos,
                                           size_t num_rows) = 0;

    virtual Status build_after_block(Block* block, size_t row_pos, size_t num_rows) = 0;
    virtual Status build_before_block(Block* before_block, const std::vector<uint32_t>& value_cids,
                                      size_t row_pos, size_t num_rows) = 0;
    virtual void clear() = 0;

    virtual std::vector<int64_t>& get_operators() = 0;

protected:
    HistoricalRowRetrieverContext _context;
};

class PrimaryKeyModelRowRetriever : public HistoricalRowRetriever {
public:
    Status init(const HistoricalRowRetrieverContext& context) override;

    Status prepare_lookup_plan_from_source_columns(
            const std::vector<IOlapColumnDataAccessor*>& key_columns,
            const IOlapColumnDataAccessor* seq_column, std::shared_ptr<MowContext> mow_context) {
        _key_columns = key_columns;
        _seq_column = seq_column;
        _mow_context = mow_context;
        return Status::OK();
    }

    Status retrieve_historical_row(const Int8* delete_sign_column_data, size_t row_pos,
                                   size_t num_rows) override;

    Status build_after_block(Block* block, size_t row_pos, size_t num_rows) override;

    Status build_before_block(Block* before_block, const std::vector<uint32_t>& value_cids,
                              size_t /*row_pos*/, size_t num_rows) override;

    void clear() override {
        _key_columns.clear();
        _seq_column = nullptr;
        _use_default_or_null_flag.clear();
        _has_default_or_nullable = false;
        _rssid_to_rid.clear();
        _rsid_to_rowset.clear();
        _operators.clear();
    }

    std::vector<int64_t>& get_operators() override { return _operators; };

private:
    void _maybe_invalid_row_cache(const std::string& key);

    // used for unique-key with merge on write and segment min_max key
    std::string _full_encode_keys(const std::vector<IOlapColumnDataAccessor*>& key_columns,
                                  size_t pos, bool null_first = true);

    std::string _full_encode_keys(const std::vector<const KeyCoder*>& key_coders,
                                  const std::vector<IOlapColumnDataAccessor*>& key_columns,
                                  size_t pos, bool null_first = true);

    // used for unique-key with merge on write
    void _encode_seq_column(const IOlapColumnDataAccessor* seq_column, size_t pos,
                            std::string* encoded_keys);

    // get key_columns, seq column, delete data from source block, prepare for searching historial data
    std::vector<IOlapColumnDataAccessor*> _key_columns;
    const IOlapColumnDataAccessor* _seq_column = nullptr;
    std::shared_ptr<MowContext> _mow_context;
    // used for building primary key index during vectorized write.
    // for mow table with cluster keys, this is cluster keys
    std::vector<const KeyCoder*> _key_coders;
    KeyCoder* _seq_coder = nullptr;

    // group every rowset-segment row id to speed up reader
    FixedReadPlan _rssid_to_rid;
    std::map<RowsetId, RowsetSharedPtr> _rsid_to_rowset;

    // cache flags for filling missing columns
    std::vector<bool> _use_default_or_null_flag;
    bool _has_default_or_nullable = false;

    // cache operator for fill_binlog_columns
    std::vector<int64_t> _operators;
};

} // namespace segment_v2
} // namespace doris
