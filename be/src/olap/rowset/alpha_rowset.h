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

#ifndef DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_H
#define DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_H

#include "olap/rowset/rowset.h"
#include "olap/rowset/segment_group.h"
#include "olap/rowset/alpha_rowset_reader.h"
#include "olap/rowset/alpha_rowset_writer.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/data_dir.h"

#include <vector>
#include <memory>

namespace doris {

class AlphaRowset : public Rowset {
public:
    AlphaRowset(const TabletSchema* schema, const std::string rowset_path,
                DataDir* data_dir, RowsetMetaSharedPtr rowset_meta);

    virtual OLAPStatus init();

    virtual std::shared_ptr<RowsetReader> create_reader();

    virtual OLAPStatus copy(RowsetWriter* dest_rowset_writer);

    virtual OLAPStatus remove();

    virtual void to_rowset_pb(RowsetMetaPB* rs_meta);

    virtual RowsetMetaSharedPtr rowset_meta() const;

    virtual void set_version(Version version);

    bool make_snapshot(std::vector<std::string>* success_links);

    bool remove_old_files(std::vector<std::string>* removed_links);

    virtual int data_disk_size() const;

    virtual int index_disk_size() const;

    virtual bool empty() const;

    virtual bool zero_num_rows() const;

    virtual size_t num_rows() const;

    virtual Version version() const;

    virtual int64_t end_version() const;

    virtual int64_t start_version() const;

    virtual VersionHash version_hash() const;

    virtual bool in_use() const;

    virtual void acquire();

    virtual void release();
    
    virtual int64_t ref_count() const;

    virtual RowsetId rowset_id() const;

    virtual void set_version_hash(VersionHash version_hash);

    virtual int64_t create_time();

    virtual bool delete_files() const;

    virtual bool is_pending() const;

    virtual int64_t txn_id() const;

private:
    OLAPStatus _init_segment_groups();

    OLAPStatus _init_pending_segment_groups();

    OLAPStatus _init_non_pending_segment_groups();

private:
    const TabletSchema* _schema;
    std::string _rowset_path;
    DataDir* _data_dir;
    RowsetMetaSharedPtr _rowset_meta;
    std::vector<std::shared_ptr<SegmentGroup>> _segment_groups;
    int _segment_group_size;
    bool _is_cumulative_rowset;
    bool _is_pending_rowset;
    atomic_t _ref_count;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_H
