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

#include "olap/delta_writer.h"

#include "olap/schema.h"
#include "olap/rowset/segment_group.h"

namespace doris {

OLAPStatus DeltaWriter::open(WriteRequest* req, DeltaWriter** writer) {
    *writer = new DeltaWriter(req);
    return OLAP_SUCCESS;
}

DeltaWriter::DeltaWriter(WriteRequest* req)
    : _req(*req), _tablet(nullptr),
      _cur_segment_group(nullptr), _new_tablet(nullptr),
      _writer(nullptr), _mem_table(nullptr),
      _schema(nullptr), _field_infos(nullptr),
      _segment_group_id(-1), _delta_written_success(false) {}

DeltaWriter::~DeltaWriter() {
    if (!_delta_written_success) {
        _garbage_collection();
    }
    SAFE_DELETE(_writer);
    SAFE_DELETE(_mem_table);
    SAFE_DELETE(_schema);
}

void DeltaWriter::_garbage_collection() {
    StorageEngine::get_instance()->delete_transaction(_req.partition_id, _req.transaction_id,
                                                   _req.tablet_id, _req.schema_hash);
    for (SegmentGroup* segment_group : _segment_group_vec) {
        segment_group->release();
        StorageEngine::get_instance()->add_unused_index(segment_group);
    }
    if (_new_tablet != nullptr) {
        StorageEngine::get_instance()->delete_transaction(_req.partition_id, _req.transaction_id,
                                                       _new_tablet->tablet_id(), _new_tablet->schema_hash());
        for (SegmentGroup* segment_group : _new_segment_group_vec) {
            segment_group->release();
            StorageEngine::get_instance()->add_unused_index(segment_group);
        }
    }
}

OLAPStatus DeltaWriter::init() {
    _tablet = StorageEngine::get_instance()->get_tablet(_req.tablet_id, _req.schema_hash);
    if (_tablet == nullptr) {
        LOG(WARNING) << "tablet_id: " << _req.tablet_id << ", "
                     << "schema_hash: " << _req.schema_hash << " not found";
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    {
        MutexLock push_lock(_tablet->get_push_lock());
        RETURN_NOT_OK(StorageEngine::get_instance()->add_transaction(
                            _req.partition_id, _req.transaction_id,
                            _req.tablet_id, _req.schema_hash, _req.load_id));
        //_segment_group_id = _tablet->current_pending_segment_group_id(_req.transaction_id);
        if (_req.need_gen_rollup) {
            TTabletId new_tablet_id;
            TSchemaHash new_schema_hash;
            _tablet->obtain_header_rdlock();
            bool is_schema_changing =
                    _tablet->get_schema_change_request(&new_tablet_id, &new_schema_hash, nullptr, nullptr);
            _tablet->release_header_lock();

            if (is_schema_changing) {
                LOG(INFO) << "load with schema change." << "old_tablet_id: " << _tablet->tablet_id() << ", "
                          << "old_schema_hash: " << _tablet->schema_hash() <<  ", "
                          << "new_tablet_id: " << new_tablet_id << ", "
                          << "new_schema_hash: " << new_schema_hash << ", "
                          << "transaction_id: " << _req.transaction_id;
                _new_tablet = StorageEngine::get_instance()->get_tablet(new_tablet_id, new_schema_hash);
                StorageEngine::get_instance()->add_transaction(
                                    _req.partition_id, _req.transaction_id,
                                    new_tablet_id, new_schema_hash, _req.load_id);
            }
        }

        // create pending data dir
        std::string dir_path = _tablet->construct_pending_data_dir_path();
        if (!check_dir_existed(dir_path)) {
            RETURN_NOT_OK(create_dirs(dir_path));
        }
    }

    ++_segment_group_id;
    //_cur_segment_group = new SegmentGroup(_tablet.get(), false, _segment_group_id, 0, true,
                               //_req.partition_id, _req.transaction_id);
    _cur_segment_group = nullptr; 
    DCHECK(_cur_segment_group != nullptr) << "failed to malloc SegmentGroup";
    _cur_segment_group->acquire();
    _cur_segment_group->set_load_id(_req.load_id);
    _segment_group_vec.push_back(_cur_segment_group);

    // New Writer to write data into SegmentGroup
    VLOG(3) << "init writer. tablet=" << _tablet->full_name() << ", "
            << "block_row_size=" << _tablet->num_rows_per_row_block();
    _writer = ColumnDataWriter::create(_cur_segment_group, true, _tablet->compress_kind(), _tablet->bloom_filter_fpp());
    DCHECK(_writer != nullptr) << "memory error occur when creating writer";

    const std::vector<SlotDescriptor*>& slots = _req.tuple_desc->slots();
    for (auto& field_info : _tablet->tablet_schema()) {
        for (size_t i = 0; i < slots.size(); ++i) {
            if (slots[i]->col_name() == field_info.name) {
                _col_ids.push_back(i);
            }
        }
    }
    _field_infos = &(_tablet->tablet_schema());
    _schema = new Schema(*_field_infos),
    _mem_table = new MemTable(_schema, _field_infos, &_col_ids,
                              _req.tuple_desc, _tablet->keys_type());
    _is_init = true;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::write(Tuple* tuple) {
    if (!_is_init) {
        auto st = init();
        if (st != OLAP_SUCCESS) {
            return st;
        }
    }

    _mem_table->insert(tuple);
    if (_mem_table->memory_usage() >= config::write_buffer_size) {
        RETURN_NOT_OK(_mem_table->flush(_writer));

        ++_segment_group_id;
        //_cur_segment_group = new SegmentGroup(_tablet.get(), false, _segment_group_id, 0, true,
        //                           _req.partition_id, _req.transaction_id);
        _cur_segment_group = nullptr; 
        DCHECK(_cur_segment_group != nullptr) << "failed to malloc SegmentGroup";
        _cur_segment_group->acquire();
        _cur_segment_group->set_load_id(_req.load_id);
        _segment_group_vec.push_back(_cur_segment_group);

        SAFE_DELETE(_writer);
        _writer = ColumnDataWriter::create(_cur_segment_group, true, _tablet->compress_kind(), _tablet->bloom_filter_fpp());
        DCHECK(_writer != nullptr) << "memory error occur when creating writer";

        SAFE_DELETE(_mem_table);
        _mem_table = new MemTable(_schema, _field_infos, &_col_ids,
                                  _req.tuple_desc, _tablet->keys_type());
    }
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::close(google::protobuf::RepeatedPtrField<PTabletInfo>* tablet_vec) {
    if (!_is_init) {
        auto st = init();
        if (st != OLAP_SUCCESS) {
            return st;
        }
    }
    RETURN_NOT_OK(_mem_table->close(_writer));

    OLAPStatus res = OLAP_SUCCESS;
    //add pending data to tablet
    RETURN_NOT_OK(_tablet->add_pending_version(_req.partition_id, _req.transaction_id, nullptr));
    for (SegmentGroup* segment_group : _segment_group_vec) {
        RETURN_NOT_OK(_tablet->add_pending_segment_group(segment_group));
        RETURN_NOT_OK(segment_group->load());
    }
    if (_new_tablet != nullptr) {
        LOG(INFO) << "convert version for schema change";
        {
            MutexLock push_lock(_new_tablet->get_push_lock());
            // create pending data dir
            std::string dir_path = _new_tablet->construct_pending_data_dir_path();
            if (!check_dir_existed(dir_path)) {
                RETURN_NOT_OK(create_dirs(dir_path));
            }
        }
        SchemaChangeHandler schema_change;
        res = schema_change.schema_version_convert(
                    _tablet, _new_tablet, &_segment_group_vec, &_new_segment_group_vec);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to convert delta for new tablet in schema change."
                << "res: " << res << ", " << "new_tablet: " << _new_tablet->full_name();
                return res;
        }

        RETURN_NOT_OK(_new_tablet->add_pending_version(_req.partition_id, _req.transaction_id, nullptr));
        for (SegmentGroup* segment_group : _new_segment_group_vec) {
            RETURN_NOT_OK(_new_tablet->add_pending_segment_group(segment_group));
            RETURN_NOT_OK(segment_group->load());
        }
    }

#ifndef BE_TEST
    PTabletInfo* tablet_info = tablet_vec->Add();
    tablet_info->set_tablet_id(_tablet->tablet_id());
    tablet_info->set_schema_hash(_tablet->schema_hash());
    if (_new_tablet != nullptr) {
        tablet_info = tablet_vec->Add();
        tablet_info->set_tablet_id(_new_tablet->tablet_id());
        tablet_info->set_schema_hash(_new_tablet->schema_hash());
    }
#endif

    _delta_written_success = true;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::cancel() {
    DCHECK(!_is_init);
    return OLAP_SUCCESS;
}

}  // namespace doris
