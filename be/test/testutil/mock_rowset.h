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
    Status create_reader(std::shared_ptr<RowsetReader>* result) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    Status remove() override { return Status::NotSupported("MockRowset not support this method."); }

    Status link_files_to(const std::string& dir, RowsetId new_rowset_id, size_t start_seg_id,
                         std::set<int64_t>* without_index_uids) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    Status copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    Status remove_old_files(std::vector<std::string>* files_to_remove) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    Status check_file_exist() override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    Status upload_to(const StorageResource& dest_fs, const RowsetId& new_rowset_id) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    void clear_inverted_index_cache() override {}

    Status get_segments_key_bounds(std::vector<KeyBoundsPB>* segments_key_bounds) override {
        // TODO(zhangchen): remove this after we implemented memrowset.
        if (is_mem_rowset_) {
            return Status::NotSupported("Memtable not support key bounds");
        }
        return Rowset::get_segments_key_bounds(segments_key_bounds);
    }

    static Status create_rowset(TabletSchemaSPtr schema, RowsetMetaSharedPtr rowset_meta,
                                RowsetSharedPtr* rowset, bool is_mem_rowset = false) {
        rowset->reset(new MockRowset(schema, rowset_meta));
        ((MockRowset*)rowset->get())->is_mem_rowset_ = is_mem_rowset;
        return Status::OK();
    }

protected:
    MockRowset(TabletSchemaSPtr schema, RowsetMetaSharedPtr rowset_meta)
            : Rowset(schema, rowset_meta, "") {}

    Status init() override { return Status::NotSupported("MockRowset not support this method."); }

    Status do_load(bool use_cache) override {
        return Status::NotSupported("MockRowset not support this method.");
    }

    void do_close() override {
        // Do nothing.
    }

    Status check_current_rowset_segment() override { return Status::OK(); }

private:
    bool is_mem_rowset_;
};

} // namespace doris
