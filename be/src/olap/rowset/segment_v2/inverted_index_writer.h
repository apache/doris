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

#include <butil/macros.h>
#include <stddef.h>
#include <stdint.h>

#include <atomic>
#include <memory>
#include <string>
#include <vector>

#include "common/config.h"
#include "common/status.h"
#include "gutil/strings/split.h"
#include "io/fs/file_system.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_common.h"
#include "olap/options.h"
#include "olap/tablet_schema.h"

namespace doris {
class CollectionValue;

class Field;

class TabletIndex;

namespace segment_v2 {
class InvertedIndexFileWriter;

class InvertedIndexColumnWriter {
public:
    static Status create(const Field* field, std::unique_ptr<InvertedIndexColumnWriter>* res,
                         InvertedIndexFileWriter* index_file_writer,
                         const TabletIndex* inverted_index);
    virtual Status init() = 0;

    InvertedIndexColumnWriter() = default;
    virtual ~InvertedIndexColumnWriter() = default;

    virtual Status add_values(const std::string name, const void* values, size_t count) = 0;
    virtual Status add_array_values(size_t field_size, const CollectionValue* values,
                                    size_t count) = 0;

    virtual Status add_array_values(size_t field_size, const void* value_ptr,
                                    const uint8_t* null_map, const uint8_t* offsets_ptr,
                                    size_t count) = 0;

    virtual Status add_nulls(uint32_t count) = 0;
    virtual Status add_array_nulls(uint32_t row_id) = 0;

    virtual Status finish() = 0;

    virtual int64_t size() const = 0;

    virtual void close_on_error() = 0;

    // check if the column is valid for inverted index, some columns
    // are generated from variant, but not all of them are supported
    static bool check_support_inverted_index(const TabletColumn& column) {
        // bellow types are not supported in inverted index for extracted columns
        static std::set<FieldType> invalid_types = {
                FieldType::OLAP_FIELD_TYPE_DOUBLE,
                FieldType::OLAP_FIELD_TYPE_JSONB,
                FieldType::OLAP_FIELD_TYPE_ARRAY,
                FieldType::OLAP_FIELD_TYPE_FLOAT,
        };
        if (column.is_extracted_column() && (invalid_types.contains(column.type()))) {
            return false;
        }
        if (column.is_variant_type()) {
            return false;
        }
        return true;
    }

private:
    DISALLOW_COPY_AND_ASSIGN(InvertedIndexColumnWriter);
};

class TmpFileDirs {
public:
    TmpFileDirs(const std::vector<doris::StorePath>& store_paths) {
        for (const auto& store_path : store_paths) {
            _tmp_file_dirs.emplace_back(store_path.path + "/" + config::tmp_file_dir);
        }
    };

    Status init() {
        for (auto& tmp_file_dir : _tmp_file_dirs) {
            // delete the tmp dir to avoid the tmp files left by last crash
            RETURN_IF_ERROR(io::global_local_filesystem()->delete_directory(tmp_file_dir));
            RETURN_IF_ERROR(io::global_local_filesystem()->create_directory(tmp_file_dir));
        }
        return Status::OK();
    };

    io::Path get_tmp_file_dir() {
        size_t cur_index = _next_index.fetch_add(1);
        return _tmp_file_dirs[cur_index % _tmp_file_dirs.size()];
    };

private:
    std::vector<io::Path> _tmp_file_dirs;
    std::atomic_size_t _next_index {0}; // use for round-robin
};

} // namespace segment_v2
} // namespace doris
