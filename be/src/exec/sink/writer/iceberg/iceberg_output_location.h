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

#include <string>
#include <string_view>
#include <utility>

#include "common/check.h"

namespace doris {

// The directory an Iceberg writer puts its files in, in both forms of the same location. BE
// creates, writes and deletes a file under write_dir, the form BE addresses the storage with.
// Iceberg metadata records the file under commit_dir, the table's own form, which every engine
// reading the table resolves. FE sends both: TIcebergTableSink output_path / original_output_path
// and TIcebergDeleteSink output_path / table_location. Example: an OSS table with data location
// oss://b/db/t/data reaches BE as write_dir s3://b/db/t/data and commit_dir oss://b/db/t/data, so
// delete_pos_1.parquet is written to s3://b/db/t/data/delete_pos_1.parquet and committed as
// oss://b/db/t/data/delete_pos_1.parquet.
//
// Every file a writer reports takes both of its paths from one IcebergOutputLocation, so it
// cannot commit the form it wrote: delete files used to be committed as s3://..., and other
// engines resolved them with a file system the table is not on.
class IcebergOutputLocation {
public:
    IcebergOutputLocation(std::string write_dir, std::string commit_dir)
            : _write_dir(std::move(write_dir)), _commit_dir(std::move(commit_dir)) {
        DORIS_CHECK(!_write_dir.empty());
        DORIS_CHECK(!_commit_dir.empty());
    }

    // The same subdirectory in both forms, e.g. the partition directory "p=1/q=2".
    IcebergOutputLocation subdirectory(std::string_view relative_dir) const {
        DORIS_CHECK(!relative_dir.empty());
        return {_join(_write_dir, relative_dir), _join(_commit_dir, relative_dir)};
    }

    // The path BE writes file_name at.
    std::string write_path(std::string_view file_name) const {
        return _join(_write_dir, file_name);
    }

    // The path Iceberg metadata records file_name at.
    std::string commit_path(std::string_view file_name) const {
        return _join(_commit_dir, file_name);
    }

private:
    // FE takes write.data.path verbatim, so a directory may end with '/'. Both forms join with
    // exactly one separator, as Iceberg's own location provider does: oss://b/t/data and
    // oss://b/t/data/ both join "p=1" into oss://b/t/data/p=1.
    static std::string _join(const std::string& dir, std::string_view name) {
        std::string path = dir;
        if (path.back() != '/') {
            path.push_back('/');
        }
        path.append(name);
        return path;
    }

    std::string _write_dir;
    std::string _commit_dir;
};

} // namespace doris
