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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ROWSET_H
#define DORIS_BE_SRC_OLAP_ROWSET_ROWSET_H

#include <memory>
#include <vector>

#include "gen_cpp/olap_file.pb.h"
#include "gutil/macros.h"
#include "olap/rowset/rowset_meta.h"
#include "util/once.h"

namespace doris {

class DataDir;
class OlapTuple;
class RowCursor;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class RowsetFactory;
class RowsetReader;
class TabletSchema;

class Rowset : public std::enable_shared_from_this<Rowset> {
public:
    virtual ~Rowset() { }

    // Open all segment files in this rowset and load necessary metadata.
    // - `use_cache` : whether to use fd cache, only applicable to alpha rowset now
    //
    // May be called multiple times, subsequent calls will no-op.
    // Derived class implements the load logic by overriding the `do_load_once()` method.
    OLAPStatus load(bool use_cache = true);

    // returns OLAP_ERR_ROWSET_CREATE_READER when failed to create reader
    virtual OLAPStatus create_reader(std::shared_ptr<RowsetReader>* result) = 0;

    // Split range denoted by `start_key` and `end_key` into sub-ranges, each contains roughly
    // `request_block_row_count` rows. Sub-range is represented by pair of OlapTuples and added to `ranges`.
    //
    // e.g., if the function generates 2 sub-ranges, the result `ranges` should contain 4 tuple: t1, t2, t2, t3.
    // Note that the end tuple of sub-range i is the same as the start tuple of sub-range i+1.
    //
    // The first/last tuple must be start_key/end_key.to_tuple(). If we can't divide the input range,
    // the result `ranges` should be [start_key.to_tuple(), end_key.to_tuple()]
    virtual OLAPStatus split_range(const RowCursor& start_key,
                                   const RowCursor& end_key,
                                   uint64_t request_block_row_count,
                                   std::vector<OlapTuple>* ranges) = 0;

    const RowsetMetaSharedPtr& rowset_meta() const { return _rowset_meta; }

    bool is_pending() const { return _is_pending; }

    // publish rowset to make it visible to read
    void make_visible(Version version, VersionHash version_hash);

    // helper class to access RowsetMeta
    int64_t start_version() const { return rowset_meta()->version().first; }
    int64_t end_version() const { return rowset_meta()->version().second; }
    VersionHash version_hash() const { return rowset_meta()->version_hash(); }
    size_t index_disk_size() const { return rowset_meta()->index_disk_size(); }
    size_t data_disk_size() const { return rowset_meta()->total_disk_size(); }
    bool empty() const { return rowset_meta()->empty(); }
    bool zero_num_rows() const { return rowset_meta()->num_rows() == 0; }
    size_t num_rows() const { return rowset_meta()->num_rows(); }
    Version version() const { return rowset_meta()->version(); }
    RowsetId rowset_id() const { return rowset_meta()->rowset_id(); }
    int64_t creation_time() { return rowset_meta()->creation_time(); }
    PUniqueId load_id() const { return rowset_meta()->load_id(); }
    int64_t txn_id() const { return rowset_meta()->txn_id(); }
    int64_t partition_id() const { return rowset_meta()->partition_id(); }
    // flag for push delete rowset
    bool delete_flag() const { return rowset_meta()->delete_flag(); }
    int64_t num_segments() const { return rowset_meta()->num_segments(); }
    void to_rowset_pb(RowsetMetaPB* rs_meta) { return rowset_meta()->to_rowset_pb(rs_meta); }

    // remove all files in this rowset
    // TODO should we rename the method to remove_files() to be more specific?
    virtual OLAPStatus remove() = 0;

    // hard link all files in this rowset to `dir` to form a new rowset with id `new_rowset_id`.
    virtual OLAPStatus link_files_to(const std::string& dir, RowsetId new_rowset_id) = 0;

    // copy all files to `dir`
    virtual OLAPStatus copy_files_to(const std::string& dir) = 0;

    virtual OLAPStatus remove_old_files(std::vector<std::string>* files_to_remove) = 0;

    // return whether `path` is one of the files in this rowset
    virtual bool check_path(const std::string& path) = 0;

    // return an unique identifier string for this rowset
    std::string unique_id() const {
        return _rowset_path + "/" + rowset_id().to_string();
    }

    bool need_delete_file() const {
        return _need_delete_file;
    }

    void set_need_delete_file() {
        _need_delete_file = true;
    }

    bool contains_version(Version version) {
        return rowset_meta()->version().first <= version.first && rowset_meta()->version().second >= version.second;
    }

    static bool comparator(const RowsetSharedPtr& left, const RowsetSharedPtr& right) {
        return left->end_version() < right->end_version();
    }

protected:
    friend class RowsetFactory;

    DISALLOW_COPY_AND_ASSIGN(Rowset);
    // this is non-public because all clients should use RowsetFactory to obtain pointer to initialized Rowset
    Rowset(const TabletSchema* schema,
           std::string rowset_path,
           RowsetMetaSharedPtr rowset_meta);

    // this is non-public because all clients should use RowsetFactory to obtain pointer to initialized Rowset
    virtual OLAPStatus init() = 0;

    // The actual implementation of load(). Guaranteed by to called exactly once.
    virtual OLAPStatus do_load_once(bool use_cache) = 0;

    // allow subclass to add custom logic when rowset is being published
    virtual void make_visible_extra(Version version, VersionHash version_hash) {}

    const TabletSchema* _schema;
    std::string _rowset_path;
    RowsetMetaSharedPtr _rowset_meta;
    // init in constructor
    bool _is_pending;    // rowset is pending iff it's not in visible state
    bool _is_cumulative; // rowset is cumulative iff it's visible and start version < end version

    DorisCallOnce<OLAPStatus> _load_once;
    bool _need_delete_file = false;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_H
