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

#include <vector>
#include <memory>

namespace doris {

class AlphaRowset : public Rowset {
public:
    AlphaRowset(const RowFields& tablet_schema,
        int num_key_fields, int num_short_key_fields,
        int num_rows_per_row_block, const std::string rowset_path,
        RowsetMetaSharedPtr rowset_meta);

    virtual OLAPStatus init();

    virtual std::unique_ptr<RowsetReader> create_reader();

    virtual OLAPStatus copy(RowsetWriter* dest_rowset_writer);

    virtual OLAPStatus remove();

    virtual RowsetMetaSharedPtr rowset_meta() const;

    virtual void set_version(Version version);

    bool create_hard_links(std::vector<std::string>* success_links);

    bool remove_old_files(std::vector<std::string>* removed_links);

    virtual int data_disk_size() const;

    virtual int index_disk_size() const;

    virtual bool empty() const;

    virtual bool zero_num_rows() const;

    virtual size_t num_rows() const;

    virtual Version version() const;

    virtual VersionHash version_hash() const;

    virtual bool in_use() const;

    virtual RowsetId rowset_id() const;

    virtual bool delete_files() const;

    virtual void set_version_hash(VersionHash version_hash);
private:
    OLAPStatus _init_segment_groups();

private:
    RowFields _tablet_schema;
    int _num_key_fields;
    int _num_short_key_fields;
    int _num_rows_per_row_block;
    std::string _rowset_path;
    RowsetMetaSharedPtr _rowset_meta;
    std::vector<std::shared_ptr<SegmentGroup>> _segment_groups;
    int _segment_group_size;
    bool _is_cumulative_rowset;
    bool _is_pending_rowset;
};

} // namespace doris

#endif // DORIS_BE_SRC_OLAP_ROWSET_ALPHA_ROWSET_H
