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

#include "olap/tuple_reader.h"

#include <parallel_hashmap/phmap.h>

#include <boost/algorithm/string/case_conv.hpp>
#include <unordered_set>

#include "olap/collect_iterator.h"
#include "olap/olap_common.h"
#include "olap/row.h"
#include "olap/row_cursor.h"
#include "runtime/mem_pool.h"

using std::nothrow;
using std::set;
using std::vector;

namespace doris {

Status TupleReader::_init_collect_iter(const ReaderParams& read_params,
                                       std::vector<RowsetReaderSharedPtr>* valid_rs_readers) {
    _collect_iter.init(this);
    std::vector<RowsetReaderSharedPtr> rs_readers;
    auto res = _capture_rs_readers(read_params, &rs_readers);
    if (!res.ok()) {
        LOG(WARNING) << "fail to init reader when _capture_rs_readers. res:" << res
                     << ", tablet_id:" << read_params.tablet->tablet_id()
                     << ", schema_hash:" << read_params.tablet->schema_hash()
                     << ", reader_type:" << read_params.reader_type
                     << ", version:" << read_params.version;
        return res;
    }

    for (auto& rs_reader : rs_readers) {
        RETURN_NOT_OK(rs_reader->init(&_reader_context));
        Status res = _collect_iter.add_child(rs_reader);
        if (!res.ok() && res.precise_code() != OLAP_ERR_DATA_EOF) {
            LOG(WARNING) << "failed to add child to iterator, err=" << res;
            return res;
        }
        if (res.ok()) {
            valid_rs_readers->push_back(rs_reader);
        }
    }
    _collect_iter.build_heap(*valid_rs_readers);
    _next_key = _collect_iter.current_row(&_next_delete_flag);

    return Status::OK();
}

Status TupleReader::init(const ReaderParams& read_params) {
    RETURN_NOT_OK(TabletReader::init(read_params));

    std::vector<RowsetReaderSharedPtr> rs_readers;
    auto status = _init_collect_iter(read_params, &rs_readers);
    if (!status.ok()) {
        return status;
    }

    if (_optimize_for_single_rowset(rs_readers)) {
        _next_row_func = _tablet->keys_type() == AGG_KEYS ? &TupleReader::_direct_agg_key_next_row
                                                          : &TupleReader::_direct_next_row;
        return Status::OK();
    }

    switch (_tablet->keys_type()) {
    case KeysType::DUP_KEYS:
        _next_row_func = &TupleReader::_direct_next_row;
        break;
    case KeysType::UNIQUE_KEYS:
        if (_tablet->enable_unique_key_merge_on_write()) {
            _next_row_func = &TupleReader::_direct_next_row;
        } else {
            _next_row_func = &TupleReader::_unique_key_next_row;
        }
        break;
    case KeysType::AGG_KEYS:
        _next_row_func = &TupleReader::_agg_key_next_row;
        break;
    default:
        DCHECK(false) << "No next row function for type:" << _tablet->keys_type();
        break;
    }

    return Status::OK();
}

Status TupleReader::_direct_next_row(RowCursor* row_cursor, MemPool* mem_pool, ObjectPool* agg_pool,
                                     bool* eof) {
    if (UNLIKELY(_next_key == nullptr)) {
        *eof = true;
        return Status::OK();
    }
    direct_copy_row(row_cursor, *_next_key);
    auto res = _collect_iter.next(&_next_key, &_next_delete_flag);
    if (UNLIKELY(!res.ok() && res.precise_code() != OLAP_ERR_DATA_EOF)) {
        return res;
    }
    return Status::OK();
}

Status TupleReader::_direct_agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool,
                                             ObjectPool* agg_pool, bool* eof) {
    if (UNLIKELY(_next_key == nullptr)) {
        *eof = true;
        return Status::OK();
    }
    init_row_with_others(row_cursor, *_next_key, mem_pool, agg_pool);
    auto res = _collect_iter.next(&_next_key, &_next_delete_flag);
    if (UNLIKELY(!res.ok() && res.precise_code() != OLAP_ERR_DATA_EOF)) {
        return res;
    }
    if (_need_agg_finalize) {
        agg_finalize_row(_value_cids, row_cursor, mem_pool);
    }
    return Status::OK();
}

Status TupleReader::_agg_key_next_row(RowCursor* row_cursor, MemPool* mem_pool,
                                      ObjectPool* agg_pool, bool* eof) {
    if (UNLIKELY(_next_key == nullptr)) {
        *eof = true;
        return Status::OK();
    }
    init_row_with_others(row_cursor, *_next_key, mem_pool, agg_pool);
    int64_t merged_count = 0;
    do {
        auto res = _collect_iter.next(&_next_key, &_next_delete_flag);
        if (UNLIKELY(res.precise_code() == OLAP_ERR_DATA_EOF)) {
            break;
        }

        if (UNLIKELY(!res.ok())) {
            LOG(WARNING) << "next failed: " << res;
            return res;
        }

        if (UNLIKELY(_aggregation && merged_count > config::doris_scanner_row_num)) {
            break;
        }

        // break while can NOT doing aggregation
        if (!equal_row(_key_cids, *row_cursor, *_next_key)) {
            break;
        }
        agg_update_row(_value_cids, row_cursor, *_next_key);
        ++merged_count;
    } while (true);
    _merged_rows += merged_count;
    // For agg query, we don't need finalize agg object and directly pass agg object to agg node
    if (_need_agg_finalize) {
        agg_finalize_row(_value_cids, row_cursor, mem_pool);
    }

    return Status::OK();
}

Status TupleReader::_unique_key_next_row(RowCursor* row_cursor, MemPool* mem_pool,
                                         ObjectPool* agg_pool, bool* eof) {
    *eof = false;
    bool cur_delete_flag = false;
    do {
        if (UNLIKELY(_next_key == nullptr)) {
            *eof = true;
            return Status::OK();
        }
        cur_delete_flag = _next_delete_flag;
        // the version is in reverse order, the first row is the highest version,
        // in UNIQUE_KEY highest version is the final result, there is no need to
        // merge the lower versions
        direct_copy_row(row_cursor, *_next_key);
        while (_next_key) {
            // skip the lower version rows;
            auto res = _collect_iter.next(&_next_key, &_next_delete_flag);
            if (LIKELY(res.precise_code() != OLAP_ERR_DATA_EOF)) {
                if (UNLIKELY(!res.ok())) {
                    LOG(WARNING) << "next failed: " << res;
                    return res;
                }

                if (!equal_row(_key_cids, *row_cursor, *_next_key)) {
                    agg_finalize_row(_value_cids, row_cursor, mem_pool);
                    break;
                }
                _merged_rows++;
                cur_delete_flag = _next_delete_flag;
            } else {
                break;
            }
        }

        // if reader needs to filter delete row and current delete_flag is true,
        // then continue
        if (!(cur_delete_flag && _filter_delete)) {
            break;
        }
        _stats.rows_del_filtered++;
    } while (cur_delete_flag);
    return Status::OK();
}

} // namespace doris
