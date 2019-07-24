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

class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;

class RowsetWriter;
class RowsetReader;

class Rowset : public std::enable_shared_from_this<Rowset> {
public:
    Rowset() : _is_inited(false), _is_loaded(false), _need_delete_file(false) {
    } 

    virtual ~Rowset() { }

    // this api is for init related objects in memory
    virtual OLAPStatus init() = 0;

    virtual bool is_inited() {
        return _is_inited;
    }

    virtual void set_inited(bool inited) {
        _is_inited = inited;
    }

    virtual bool is_loaded() {
        return _is_loaded;
    }

    void set_loaded(bool loaded) {
        _is_loaded= loaded;
    }

    // this api is for lazy loading data
    // always means that there are some io
    virtual OLAPStatus load(bool use_cache = true) = 0;

    virtual std::shared_ptr<RowsetReader> create_reader() = 0;

    virtual OLAPStatus remove() = 0;

    virtual void to_rowset_pb(RowsetMetaPB* rs_meta) = 0;

    virtual RowsetMetaSharedPtr rowset_meta() const = 0;

    virtual size_t data_disk_size() const = 0;

    virtual size_t index_disk_size() const = 0;

    virtual bool empty() const = 0;

    virtual bool zero_num_rows() const = 0;

    virtual size_t num_rows() const = 0;

    virtual Version version() const = 0;

    virtual void set_version_and_version_hash(Version version, VersionHash version_hash) = 0;

    virtual int64_t end_version() const = 0;

    virtual int64_t start_version() const = 0;

    virtual VersionHash version_hash() const = 0;

    virtual bool in_use() const = 0;

    virtual void acquire() = 0;

    virtual void release() = 0;
    
    virtual int64_t ref_count() const = 0;

    virtual OLAPStatus make_snapshot(const std::string& snapshot_path,
                                     std::vector<std::string>* success_links) = 0;
    virtual OLAPStatus copy_files_to_path(const std::string& dest_path,
                                          std::vector<std::string>* success_files) = 0;

    virtual OLAPStatus remove_old_files(std::vector<std::string>* files_to_remove) = 0;

    virtual RowsetId rowset_id() const = 0;

    virtual int64_t creation_time() = 0;

    virtual bool is_pending() const = 0;

    virtual PUniqueId load_id() const = 0;

    virtual int64_t txn_id() const = 0;

    virtual int64_t partition_id() const = 0;
    
    // flag for push delete rowset
    virtual bool delete_flag() = 0;

    virtual bool check_path(const std::string& path) = 0;

    virtual std::string unique_id() = 0;

    bool need_delete_file() {
        return _need_delete_file;
    }

    void set_need_delete_file(bool need_delete_file) {
        if (_need_delete_file == true) {
            return;
        }
        _need_delete_file = need_delete_file;
    }

private:
    bool _is_inited;
    bool _is_loaded;
    bool _need_delete_file;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ROWSET_H
