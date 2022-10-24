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

#include <atomic>
#include <memory>
#include <mutex>
#include <vector>

#include "env/env.h"
#include "gen_cpp/olap_file.pb.h"
#include "gutil/macros.h"
#include "io/fs/remote_file_system.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/tablet_schema.h"

namespace doris {

class DataDir;
class OlapTuple;
class RowCursor;
class Rowset;
using RowsetSharedPtr = std::shared_ptr<Rowset>;
class RowsetFactory;
class RowsetReader;

// the rowset state transfer graph:
//    ROWSET_UNLOADED    <--|
//          ↓               |
//    ROWSET_LOADED         |
//          ↓               |
//    ROWSET_UNLOADING   -->|
enum RowsetState {
    // state for new created rowset
    ROWSET_UNLOADED,
    // state after load() called
    ROWSET_LOADED,
    // state for closed() called but owned by some readers
    ROWSET_UNLOADING
};

class RowsetStateMachine {
public:
    RowsetStateMachine() : _rowset_state(ROWSET_UNLOADED) {}

    Status on_load() {
        switch (_rowset_state) {
        case ROWSET_UNLOADED:
            _rowset_state = ROWSET_LOADED;
            break;

        default:
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION);
        }
        return Status::OK();
    }

    Status on_close(uint64_t refs_by_reader) {
        switch (_rowset_state) {
        case ROWSET_LOADED:
            if (refs_by_reader == 0) {
                _rowset_state = ROWSET_UNLOADED;
            } else {
                _rowset_state = ROWSET_UNLOADING;
            }
            break;

        default:
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION);
        }
        return Status::OK();
    }

    Status on_release() {
        switch (_rowset_state) {
        case ROWSET_UNLOADING:
            _rowset_state = ROWSET_UNLOADED;
            break;

        default:
            return Status::OLAPInternalError(OLAP_ERR_ROWSET_INVALID_STATE_TRANSITION);
        }
        return Status::OK();
    }

    RowsetState rowset_state() { return _rowset_state; }

private:
    RowsetState _rowset_state;
};

class Rowset : public std::enable_shared_from_this<Rowset> {
public:
    virtual ~Rowset() = default;

    // Open all segment files in this rowset and load necessary metadata.
    // - `use_cache` : whether to use fd cache, only applicable to alpha rowset now
    //
    // May be called multiple times, subsequent calls will no-op.
    // Derived class implements the load logic by overriding the `do_load_once()` method.
    Status load(bool use_cache = true);

    // returns Status::OLAPInternalError(OLAP_ERR_ROWSET_CREATE_READER) when failed to create reader
    virtual Status create_reader(std::shared_ptr<RowsetReader>* result) = 0;

    // Split range denoted by `start_key` and `end_key` into sub-ranges, each contains roughly
    // `request_block_row_count` rows. Sub-range is represented by pair of OlapTuples and added to `ranges`.
    //
    // e.g., if the function generates 2 sub-ranges, the result `ranges` should contain 4 tuple: t1, t2, t2, t3.
    // Note that the end tuple of sub-range i is the same as the start tuple of sub-range i+1.
    //
    // The first/last tuple must be start_key/end_key.to_tuple(). If we can't divide the input range,
    // the result `ranges` should be [start_key.to_tuple(), end_key.to_tuple()]
    virtual Status split_range(const RowCursor& start_key, const RowCursor& end_key,
                               uint64_t request_block_row_count, size_t key_num,
                               std::vector<OlapTuple>* ranges) = 0;

    const RowsetMetaSharedPtr& rowset_meta() const { return _rowset_meta; }

    bool is_pending() const { return _is_pending; }

    bool is_local() const { return _rowset_meta->is_local(); }

    // publish rowset to make it visible to read
    void make_visible(Version version);
    TabletSchemaSPtr tablet_schema() { return _schema; }

    // helper class to access RowsetMeta
    int64_t start_version() const { return rowset_meta()->version().first; }
    int64_t end_version() const { return rowset_meta()->version().second; }
    size_t index_disk_size() const { return rowset_meta()->index_disk_size(); }
    size_t data_disk_size() const { return rowset_meta()->total_disk_size(); }
    bool empty() const { return rowset_meta()->empty(); }
    bool zero_num_rows() const { return rowset_meta()->num_rows() == 0; }
    size_t num_rows() const { return rowset_meta()->num_rows(); }
    Version version() const { return rowset_meta()->version(); }
    RowsetId rowset_id() const { return rowset_meta()->rowset_id(); }
    int64_t creation_time() const { return rowset_meta()->creation_time(); }
    PUniqueId load_id() const { return rowset_meta()->load_id(); }
    int64_t txn_id() const { return rowset_meta()->txn_id(); }
    int64_t partition_id() const { return rowset_meta()->partition_id(); }
    // flag for push delete rowset
    bool delete_flag() const { return rowset_meta()->delete_flag(); }
    int64_t num_segments() const { return rowset_meta()->num_segments(); }
    void to_rowset_pb(RowsetMetaPB* rs_meta) const { return rowset_meta()->to_rowset_pb(rs_meta); }
    RowsetMetaPB get_rowset_pb() const { return rowset_meta()->get_rowset_pb(); }
    int64_t oldest_write_timestamp() const { return rowset_meta()->oldest_write_timestamp(); }
    int64_t newest_write_timestamp() const { return rowset_meta()->newest_write_timestamp(); }
    KeysType keys_type() { return _schema->keys_type(); }

    // remove all files in this rowset
    // TODO should we rename the method to remove_files() to be more specific?
    virtual Status remove() = 0;

    // close to clear the resource owned by rowset
    // including: open files, indexes and so on
    // NOTICE: can not call this function in multithreads
    void close() {
        RowsetState old_state = _rowset_state_machine.rowset_state();
        if (old_state != ROWSET_LOADED) {
            return;
        }
        Status st = Status::OK();
        {
            std::lock_guard<std::mutex> close_lock(_lock);
            uint64_t current_refs = _refs_by_reader;
            old_state = _rowset_state_machine.rowset_state();
            if (old_state != ROWSET_LOADED) {
                return;
            }
            if (current_refs == 0) {
                do_close();
            }
            st = _rowset_state_machine.on_close(current_refs);
        }
        if (!st.ok()) {
            LOG(WARNING) << "state transition failed from:" << _rowset_state_machine.rowset_state();
            return;
        }
        VLOG_NOTICE << "rowset is close. rowset state from:" << old_state << " to "
                    << _rowset_state_machine.rowset_state() << ", version:" << start_version()
                    << "-" << end_version() << ", tabletid:" << _rowset_meta->tablet_id();
    }

    // hard link all files in this rowset to `dir` to form a new rowset with id `new_rowset_id`.
    virtual Status link_files_to(const std::string& dir, RowsetId new_rowset_id) = 0;

    // copy all files to `dir`
    virtual Status copy_files_to(const std::string& dir, const RowsetId& new_rowset_id) = 0;

    virtual Status upload_to(io::RemoteFileSystem* dest_fs, const RowsetId& new_rowset_id) {
        return Status::OK();
    }

    virtual Status remove_old_files(std::vector<std::string>* files_to_remove) = 0;

    // return whether `path` is one of the files in this rowset
    virtual bool check_path(const std::string& path) = 0;

    virtual bool check_file_exist() = 0;

    // return an unique identifier string for this rowset
    std::string unique_id() const {
        return fmt::format("{}/{}", _tablet_path, rowset_id().to_string());
    }

    bool need_delete_file() const { return _need_delete_file; }

    void set_need_delete_file() { _need_delete_file = true; }

    bool contains_version(Version version) const {
        return rowset_meta()->version().contains(version);
    }

    const std::string& tablet_path() const { return _tablet_path; }

    virtual std::string rowset_dir() { return _rowset_dir; }

    static bool comparator(const RowsetSharedPtr& left, const RowsetSharedPtr& right) {
        return left->end_version() < right->end_version();
    }

    // this function is called by reader to increase reference of rowset
    void acquire() { ++_refs_by_reader; }

    void release() {
        // if the refs by reader is 0 and the rowset is closed, should release the resouce
        uint64_t current_refs = --_refs_by_reader;
        if (current_refs == 0 && _rowset_state_machine.rowset_state() == ROWSET_UNLOADING) {
            {
                std::lock_guard<std::mutex> release_lock(_lock);
                // rejudge _refs_by_reader because we do not add lock in create reader
                if (_refs_by_reader == 0 &&
                    _rowset_state_machine.rowset_state() == ROWSET_UNLOADING) {
                    // first do close, then change state
                    do_close();
                    _rowset_state_machine.on_release();
                }
            }
            if (_rowset_state_machine.rowset_state() == ROWSET_UNLOADED) {
                VLOG_NOTICE
                        << "close the rowset. rowset state from ROWSET_UNLOADING to ROWSET_UNLOADED"
                        << ", version:" << start_version() << "-" << end_version()
                        << ", tabletid:" << _rowset_meta->tablet_id();
            }
        }
    }

    virtual Status get_segments_key_bounds(std::vector<KeyBoundsPB>* segments_key_bounds) {
        _rowset_meta->get_segments_key_bounds(segments_key_bounds);
        return Status::OK();
    }

    bool check_rowset_segment();

protected:
    friend class RowsetFactory;

    DISALLOW_COPY_AND_ASSIGN(Rowset);
    // this is non-public because all clients should use RowsetFactory to obtain pointer to initialized Rowset
    Rowset(TabletSchemaSPtr schema, const std::string& tablet_path,
           RowsetMetaSharedPtr rowset_meta);

    // this is non-public because all clients should use RowsetFactory to obtain pointer to initialized Rowset
    virtual Status init() = 0;

    // The actual implementation of load(). Guaranteed by to called exactly once.
    virtual Status do_load(bool use_cache) = 0;

    // release resources in this api
    virtual void do_close() = 0;

    // allow subclass to add custom logic when rowset is being published
    virtual void make_visible_extra(Version version) {}

    virtual bool check_current_rowset_segment() = 0;

    TabletSchemaSPtr _schema;

    std::string _tablet_path;
    std::string _rowset_dir;
    RowsetMetaSharedPtr _rowset_meta;
    // init in constructor
    bool _is_pending;    // rowset is pending iff it's not in visible state
    bool _is_cumulative; // rowset is cumulative iff it's visible and start version < end version

    // mutex lock for load/close api because it is costly
    std::mutex _lock;
    bool _need_delete_file = false;
    // variable to indicate how many rowset readers owned this rowset
    std::atomic<uint64_t> _refs_by_reader;
    // rowset state machine
    RowsetStateMachine _rowset_state_machine;
};

} // namespace doris
