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

#ifndef DORIS_BE_SRC_OLAP_BASE_TABLET_H
#define DORIS_BE_SRC_OLAP_BASE_TABLET_H

#include <memory>

#include "gen_cpp/AgentService_types.h"
#include "gen_cpp/MasterService_types.h"
#include "olap/data_dir.h"
#include "olap/olap_define.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
#include "util/once.h"

namespace doris {

class DataDir;
class BaseTablet;
using BaseTabletSharedPtr = std::shared_ptr<BaseTablet>;

// Base class for all tablet classes, currently only olap/Tablet and
// olap/memory/MemTablet.
// The fields and methods in this class is not final, it will change as memory
// storage engine evolves.
class BaseTablet : public std::enable_shared_from_this<BaseTablet> {
public:
    BaseTablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir);
    virtual ~BaseTablet();

    inline DataDir* data_dir() const;
    std::string tablet_path() const;

    TabletState tablet_state() const { return _state; }
    OLAPStatus set_tablet_state(TabletState state);

    // Property encapsulated in TabletMeta
    inline const TabletMetaSharedPtr tablet_meta();

    inline bool is_memory() const;
    inline TabletUid tablet_uid() const;
    inline int64_t table_id() const;
    // Returns a string can be used to uniquely identify a tablet.
    // The result string will often be printed to the log.
    inline const std::string full_name() const;
    inline int64_t partition_id() const;
    inline int64_t tablet_id() const;
    inline int32_t schema_hash() const;
    inline int16_t shard_id();
    inline const int64_t creation_time() const;
    inline void set_creation_time(int64_t creation_time);
    inline bool equal(int64_t tablet_id, int32_t schema_hash);

    OLAPStatus init();
    inline bool init_succeeded();

    bool is_used();

    void save_meta();

    void register_tablet_into_dir();
    void deregister_tablet_from_dir();


    // properties encapsulated in TabletSchema
    inline const TabletSchema& tablet_schema() const;
    inline size_t tablet_footprint(); // disk space occupied by tablet
    inline size_t num_rows();
    inline int version_count() const;
    inline Version max_version() const;

    // propreties encapsulated in TabletSchema
    inline KeysType keys_type() const;
    inline size_t num_columns() const;
    inline size_t num_null_columns() const;
    inline size_t num_key_columns() const;
    inline size_t num_short_key_columns() const;
    inline size_t num_rows_per_row_block() const;
    inline CompressKind compress_kind() const;
    inline double bloom_filter_fpp() const;
    inline size_t next_unique_id() const;
    inline size_t row_size() const;
    inline size_t field_index(const string& field_name) const;

    OLAPStatus set_partition_id(int64_t partition_id);

    TabletInfo get_tablet_info() const;

    // meta lock
    inline void obtain_header_rdlock() { _meta_lock.rdlock(); }
    inline void obtain_header_wrlock() { _meta_lock.wrlock(); }
    inline void release_header_lock() { _meta_lock.unlock(); }
    inline RWMutex* get_header_lock_ptr() { return &_meta_lock; }

    virtual void build_tablet_report_info(TTabletInfo* tablet_info) = 0;

    virtual void delete_all_files() = 0;

protected:
    void _gen_tablet_path();
    virtual OLAPStatus _init_once_action() = 0;

protected:
    TabletState _state;
    TabletMetaSharedPtr _tablet_meta;
    TabletSchema _schema;

    DataDir* _data_dir;
    std::string _tablet_path;

    DorisCallOnce<OLAPStatus> _init_once;
    // TODO(lingbin): There is a _meta_lock TabletMeta too, there should be a comment to
    // explain how these two locks work together.
    mutable RWMutex _meta_lock;
    // if this tablet is broken, set to true. default is false
    std::atomic<bool> _is_bad;

private:
    DISALLOW_COPY_AND_ASSIGN(BaseTablet);
};

inline DataDir* BaseTablet::data_dir() const {
    return _data_dir;
}

inline string BaseTablet::tablet_path() const {
    return _tablet_path;
}

inline const TabletMetaSharedPtr BaseTablet::tablet_meta() {
    return _tablet_meta;
}

inline bool BaseTablet::is_memory() const {
    return _tablet_meta->tablet_type() == TabletTypePB::TABLET_TYPE_MEMORY;
}

inline TabletUid BaseTablet::tablet_uid() const {
    return _tablet_meta->tablet_uid();
}

inline int64_t BaseTablet::table_id() const {
    return _tablet_meta->table_id();
}

inline const std::string BaseTablet::full_name() const {
    std::stringstream ss;
    ss << _tablet_meta->tablet_id() << "." << _tablet_meta->schema_hash() << "."
       << _tablet_meta->tablet_uid().to_string();
    return ss.str();
}

inline int64_t BaseTablet::partition_id() const {
    return _tablet_meta->partition_id();
}

inline int64_t BaseTablet::tablet_id() const {
    return _tablet_meta->tablet_id();
}

inline int32_t BaseTablet::schema_hash() const {
    return _tablet_meta->schema_hash();
}

inline int16_t BaseTablet::shard_id() {
    return _tablet_meta->shard_id();
}

inline const int64_t BaseTablet::creation_time() const {
    return _tablet_meta->creation_time();
}

inline void BaseTablet::set_creation_time(int64_t creation_time) {
    _tablet_meta->set_creation_time(creation_time);
}

inline bool BaseTablet::equal(int64_t id, int32_t hash) {
    return (tablet_id() == id) && (schema_hash() == hash);
}

inline const TabletSchema& BaseTablet::tablet_schema() const {
    return _schema;
}

inline bool BaseTablet::init_succeeded() {
    return _init_once.has_called() && _init_once.stored_result() == OLAP_SUCCESS;
}

inline bool BaseTablet::is_used() {
    return !_is_bad && _data_dir->is_used();
}

inline void BaseTablet::register_tablet_into_dir() {
    _data_dir->register_tablet(this);
}

inline void BaseTablet::deregister_tablet_from_dir() {
    _data_dir->deregister_tablet(this);
}

// TODO(lingbin): Why other methods that need to get information from _tablet_meta
// are not locked, here needs a comment to explain.
inline size_t BaseTablet::tablet_footprint() {
    ReadLock rdlock(&_meta_lock);
    return _tablet_meta->tablet_footprint();
}

// TODO(lingbin): Why other methods which need to get information from _tablet_meta
// are not locked, here needs a comment to explain.
inline size_t BaseTablet::num_rows() {
    ReadLock rdlock(&_meta_lock);
    return _tablet_meta->num_rows();
}

inline int BaseTablet::version_count() const {
    return _tablet_meta->version_count();
}

inline Version BaseTablet::max_version() const {
    return _tablet_meta->max_version();
}

inline KeysType BaseTablet::keys_type() const {
    return _schema.keys_type();
}

inline size_t BaseTablet::num_columns() const {
    return _schema.num_columns();
}

inline size_t BaseTablet::num_null_columns() const {
    return _schema.num_null_columns();
}

inline size_t BaseTablet::num_key_columns() const {
    return _schema.num_key_columns();
}

inline size_t BaseTablet::num_short_key_columns() const {
    return _schema.num_short_key_columns();
}

inline size_t BaseTablet::num_rows_per_row_block() const {
    return _schema.num_rows_per_row_block();
}

inline CompressKind BaseTablet::compress_kind() const {
    return _schema.compress_kind();
}

inline double BaseTablet::bloom_filter_fpp() const {
    return _schema.bloom_filter_fpp();
}

inline size_t BaseTablet::next_unique_id() const {
    return _schema.next_column_unique_id();
}

inline size_t BaseTablet::field_index(const string& field_name) const {
    return _schema.field_index(field_name);
}

inline size_t BaseTablet::row_size() const {
    return _schema.row_size();
}

} /* namespace doris */

#endif /* DORIS_BE_SRC_OLAP_BASE_TABLET_H */
