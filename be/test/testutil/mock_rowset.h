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

#include "olap/rowset/rowset.h"
#include "olap/tablet_schema.h"

namespace doris {

class MockRowset : public Rowset {
    virtual Status create_reader(std::shared_ptr<RowsetReader>* result) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual Status split_range(const RowCursor& start_key, const RowCursor& end_key,
                               uint64_t request_block_row_count, size_t key_num,
                               std::vector<OlapTuple>* ranges) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual Status remove() override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual Status link_files_to(const std::string& dir, RowsetId new_rowset_id) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual Status copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual Status remove_old_files(std::vector<std::string>* files_to_remove) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual bool check_path(const std::string& path) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual bool check_file_exist() override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual Status get_segments_key_bounds(std::vector<KeyBoundsPB>* segments_key_bounds) override {
        // TODO(zhangchen): remove this after we implemented memrowset.
        if (is_mem_rowset_) {
            return Status::NotSupported("Memtable not support key bounds");
        }
        return Rowset::get_segments_key_bounds(segments_key_bounds);
    }

    static Status create_rowset(TabletSchemaSPtr schema, const std::string& rowset_path,
                                RowsetMetaSharedPtr rowset_meta, RowsetSharedPtr* rowset,
                                bool is_mem_rowset = false) {
        rowset->reset(new MockRowset(schema, rowset_path, rowset_meta));
        ((MockRowset*)rowset->get())->is_mem_rowset_ = is_mem_rowset;
        return Status::OK();
    }

protected:
    MockRowset(TabletSchemaSPtr schema, const std::string& rowset_path,
               RowsetMetaSharedPtr rowset_meta)
            : Rowset(schema, rowset_path, rowset_meta) {}

    virtual Status init() override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual Status do_load(bool use_cache) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    virtual void do_close() override {
        // Do nothing.
    }

    virtual bool check_current_rowset_segment() override { return true; };

private:
    bool is_mem_rowset_;
};

} // namespace doris
