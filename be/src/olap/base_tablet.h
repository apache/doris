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

#include "olap/olap_define.h"
#include "olap/tablet_meta.h"
#include "olap/utils.h"
#include "util/metrics.h"

namespace doris {

class DataDir;

// Base class for all tablet classes, currently only olap/Tablet
// The fields and methods in this class is not final, it will change as memory
// storage engine evolves.
class BaseTablet : public std::enable_shared_from_this<BaseTablet> {
public:
    BaseTablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir);
    virtual ~BaseTablet();

    inline DataDir* data_dir() const;
    FilePathDesc tablet_path_desc() const;

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

    // properties encapsulated in TabletSchema
    inline const TabletSchema& tablet_schema() const;

protected:
    void _gen_tablet_path();

protected:
    TabletState _state;
    TabletMetaSharedPtr _tablet_meta;
    const TabletSchema& _schema;

    DataDir* _data_dir;
    FilePathDesc _tablet_path_desc;

    // metrics of this tablet
    std::shared_ptr<MetricEntity> _metric_entity = nullptr;

    std::string _full_name;

public:
    IntCounter* query_scan_bytes;
    IntCounter* query_scan_rows;
    IntCounter* query_scan_count;

private:
    DISALLOW_COPY_AND_ASSIGN(BaseTablet);
};

inline DataDir* BaseTablet::data_dir() const {
    return _data_dir;
}

inline FilePathDesc BaseTablet::tablet_path_desc() const {
    return _tablet_path_desc;
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
    return _full_name;
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

} /* namespace doris */

#endif /* DORIS_BE_SRC_OLAP_BASE_TABLET_H */
