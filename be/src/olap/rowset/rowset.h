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

#include "gen_cpp/olap_file.pb.h"
#include "olap/new_status.h"
#include "olap/rowset/rowset_meta.h"

#include <memory>

namespace doris {

class DataDir;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class RowsetReader;
class TabletSchema;

class Rowset : public std::enable_shared_from_this<Rowset> {
public:
    Rowset(const TabletSchema* schema,
           std::string rowset_path,
           DataDir* data_dir,
           RowsetMetaSharedPtr rowset_meta);

    virtual ~Rowset() { }

    // this api is for init related objects in memory
    virtual OLAPStatus init() = 0;

    bool is_inited() const {
        return _is_inited;
    }

    void set_inited(bool inited) {
        _is_inited = inited;
    }

    bool is_loaded() const {
        return _is_loaded;
    }

    void set_loaded(bool loaded) {
        _is_loaded= loaded;
    }

    RowsetMetaSharedPtr rowset_meta() const {
        return _rowset_meta;
    }

    bool is_pending() const {
        return _is_pending;
    }

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

    // this api is for lazy loading data
    // always means that there are some io
    virtual OLAPStatus load(bool use_cache = true) = 0;

    virtual std::shared_ptr<RowsetReader> create_reader() = 0;

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
        return _rowset_path + "/" + std::to_string(rowset_id());
    }

    bool need_delete_file() const {
        return _need_delete_file;
    }

    void set_need_delete_file() {
        _need_delete_file = true;
    }

    static bool comparator(const RowsetSharedPtr& left, const RowsetSharedPtr& right) {
        return left->end_version() < right->end_version();
    }

protected:
    // allow subclass to add custom logic when rowset is being published
    virtual void make_visible_extra(Version version, VersionHash version_hash) {}

    const TabletSchema* _schema;
    std::string _rowset_path;
    DataDir* _data_dir;
    RowsetMetaSharedPtr _rowset_meta;
    // init in constructor
    bool _is_pending;    // rowset is pending iff it's not in visible state
    bool _is_cumulative; // rowset is cumulative iff it's visible and start version < end version

    bool _is_inited = false;
    bool _is_loaded = false;
    bool _need_delete_file = false;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_H
