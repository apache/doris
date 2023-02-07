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

#include <memory>
#include <string>

#include "olap/olap_define.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/metrics.h"

namespace doris {

class DataDir;

// Base class for all tablet classes, currently only olap/Tablet
// The fields and methods in this class is not final, it will change as memory
// storage engine evolves.
class BaseTablet {
public:
    BaseTablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir);
    virtual ~BaseTablet();

    DataDir* data_dir() const;
    const std::string& tablet_path() const;

    TabletState tablet_state() const { return _state; }
    Status set_tablet_state(TabletState state);

    // Property encapsulated in TabletMeta
    const TabletMetaSharedPtr& tablet_meta();

    bool is_memory() const;
    TabletUid tablet_uid() const;
    int64_t table_id() const;
    // Returns a string can be used to uniquely identify a tablet.
    // The result string will often be printed to the log.
    const std::string full_name() const;
    int64_t partition_id() const;
    int64_t tablet_id() const;
    int64_t replica_id() const;
    int32_t schema_hash() const;
    int16_t shard_id() const;

    int64_t storage_policy_id() const { return _tablet_meta->storage_policy_id(); }

    void set_storage_policy_id(int64_t id) { _tablet_meta->set_storage_policy_id(id); }

    // properties encapsulated in TabletSchema
    virtual TabletSchemaSPtr tablet_schema() const;

    bool set_tablet_schema_into_rowset_meta();

protected:
    void _gen_tablet_path();

protected:
    TabletState _state;
    TabletMetaSharedPtr _tablet_meta;
    TabletSchemaSPtr _schema;

    DataDir* _data_dir;
    std::string _tablet_path;

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

inline const std::string& BaseTablet::tablet_path() const {
    return _tablet_path;
}

inline const TabletMetaSharedPtr& BaseTablet::tablet_meta() {
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

inline int64_t BaseTablet::replica_id() const {
    return _tablet_meta->replica_id();
}

inline int32_t BaseTablet::schema_hash() const {
    return _tablet_meta->schema_hash();
}

inline int16_t BaseTablet::shard_id() const {
    return _tablet_meta->shard_id();
}

inline TabletSchemaSPtr BaseTablet::tablet_schema() const {
    return _schema;
}

} /* namespace doris */
