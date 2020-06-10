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

#include "olap/memory/mem_tablet.h"

#include "olap/memory/mem_sub_tablet.h"
#include "olap/memory/mem_tablet_scan.h"
#include "olap/memory/write_txn.h"

namespace doris {
namespace memory {

MemTablet::MemTablet(TabletMetaSharedPtr tablet_meta, DataDir* data_dir)
        : BaseTablet(tablet_meta, data_dir) {
    _mem_schema.reset(new Schema(_schema));
}

MemTablet::~MemTablet() {}

std::shared_ptr<MemTablet> MemTablet::create_tablet_from_meta(TabletMetaSharedPtr tablet_meta,
                                                              DataDir* data_dir) {
    return std::make_shared<MemTablet>(tablet_meta, data_dir);
}

Status MemTablet::init() {
    _max_version = 0;
    return MemSubTablet::create(0, *_mem_schema.get(), &_sub_tablet);
}

Status MemTablet::scan(std::unique_ptr<ScanSpec>* spec, std::unique_ptr<MemTabletScan>* scan) {
    uint64_t version = (*spec)->version();
    if (version == UINT64_MAX) {
        version = _max_version;
        (*spec)->_version = version;
    }
    if (version > _max_version) {
        return Status::InvalidArgument("Illegal scan version (larger than latest version)");
    }
    size_t num_rows = 0;
    RETURN_IF_ERROR(_sub_tablet->get_size(version, &num_rows));
    num_rows = std::min((*spec)->_limit, num_rows);
    std::vector<std::unique_ptr<ColumnReader>> readers;
    auto& columns = (*spec)->columns();
    readers.resize(columns.size());
    for (size_t i = 0; i < columns.size(); ++i) {
        const ColumnSchema* cs = _mem_schema->get_by_name(columns[i]);
        if (!cs) {
            return Status::NotFound("column not found for scan");
        }
        RETURN_IF_ERROR(_sub_tablet->read_column(version, cs->cid(), &readers[i]));
    }
    scan->reset(new MemTabletScan(std::static_pointer_cast<MemTablet>(shared_from_this()), spec,
                                  num_rows, &readers));
    return Status::OK();
}

Status MemTablet::create_write_txn(std::unique_ptr<WriteTxn>* wtxn) {
    wtxn->reset(new WriteTxn(&_mem_schema));
    return Status::OK();
}

Status MemTablet::commit_write_txn(WriteTxn* wtxn, uint64_t version) {
    std::lock_guard<std::mutex> lg(_write_lock);
    DCHECK_LT(_max_version, version);
    RETURN_IF_ERROR(_sub_tablet->begin_write(&_mem_schema));
    for (size_t i = 0; i < wtxn->batch_size(); i++) {
        auto batch = wtxn->get_batch(i);
        RETURN_IF_ERROR(_sub_tablet->apply_partial_row_batch(batch));
    }
    RETURN_IF_ERROR(_sub_tablet->commit_write(version));
    _max_version = version;
    return Status::OK();
}

} // namespace memory
} // namespace doris
