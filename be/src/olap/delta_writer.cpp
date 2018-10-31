// Copyright (c) 2018, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
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
#include "olap/rowset.h"

namespace palo {

OLAPStatus DeltaWriter::open(WriteRequest* req, DeltaWriter** writer) {
    *writer = new DeltaWriter(req);
    return OLAP_SUCCESS;
}

DeltaWriter::DeltaWriter(WriteRequest* req)
    : _req(*req), _table(nullptr),
      _cur_rowset(nullptr), _new_table(nullptr),
      _writer(nullptr), _mem_table(nullptr),
      _schema(nullptr), _field_infos(nullptr),
      _rowset_id(-1), _delta_written_success(false) {}

DeltaWriter::~DeltaWriter() {
    if (!_delta_written_success) {
        _garbage_collection();
    }
    SAFE_DELETE(_writer);
    SAFE_DELETE(_mem_table);
    SAFE_DELETE(_schema);
}

void DeltaWriter::_garbage_collection() {
    OLAPEngine::get_instance()->delete_transaction(_req.partition_id, _req.transaction_id,
                                                   _req.tablet_id, _req.schema_hash);
    for (Rowset* rowset : _rowset_vec) {
        rowset->release();
        OLAPEngine::get_instance()->add_unused_index(rowset);
    }
    if (_new_table != nullptr) {
        OLAPEngine::get_instance()->delete_transaction(_req.partition_id, _req.transaction_id,
                                                       _new_table->tablet_id(), _new_table->schema_hash());
        for (Rowset* rowset : _new_rowset_vec) {
            rowset->release();
            OLAPEngine::get_instance()->add_unused_index(rowset);
        }
    }
}

OLAPStatus DeltaWriter::init() {
    _table = OLAPEngine::get_instance()->get_table(_req.tablet_id, _req.schema_hash);
    if (_table == nullptr) {
        LOG(WARNING) << "tablet_id: " << _req.tablet_id << ", "
                     << "schema_hash: " << _req.schema_hash << " not found";
        return OLAP_ERR_TABLE_NOT_FOUND;
    }

    {
        MutexLock push_lock(_table->get_push_lock());
        RETURN_NOT_OK(OLAPEngine::get_instance()->add_transaction(
                            _req.partition_id, _req.transaction_id,
                            _req.tablet_id, _req.schema_hash, _req.load_id));
        //_rowset_id = _table->current_pending_rowset_id(_req.transaction_id);
        if (_req.need_gen_rollup) {
            TTabletId new_tablet_id;
            TSchemaHash new_schema_hash;
            _table->obtain_header_rdlock();
            bool is_schema_changing =
                    _table->get_schema_change_request(&new_tablet_id, &new_schema_hash, nullptr, nullptr);
            _table->release_header_lock();

            if (is_schema_changing) {
                LOG(INFO) << "load with schema change." << "old_tablet_id: " << _table->tablet_id() << ", "
                          << "old_schema_hash: " << _table->schema_hash() <<  ", "
                          << "new_tablet_id: " << new_tablet_id << ", "
                          << "new_schema_hash: " << new_schema_hash << ", "
                          << "transaction_id: " << _req.transaction_id;
                _new_table = OLAPEngine::get_instance()->get_table(new_tablet_id, new_schema_hash);
                OLAPEngine::get_instance()->add_transaction(
                                    _req.partition_id, _req.transaction_id,
                                    new_tablet_id, new_schema_hash, _req.load_id);
            }
        }

        // create pending data dir
        std::string dir_path = _table->construct_pending_data_dir_path();
        if (!check_dir_existed(dir_path)) {
            RETURN_NOT_OK(create_dirs(dir_path));
        }
    }

    ++_rowset_id;
    _cur_rowset = new Rowset(_table.get(), false, _rowset_id, 0, true,
                               _req.partition_id, _req.transaction_id);
    DCHECK(_cur_rowset != nullptr) << "failed to malloc Rowset";
    _cur_rowset->acquire();
    _cur_rowset->set_load_id(_req.load_id);
    _rowset_vec.push_back(_cur_rowset);

    // New Writer to write data into Rowset
    VLOG(3) << "init writer. table=" << _table->full_name() << ", "
            << "block_row_size=" << _table->num_rows_per_row_block();
    _writer = IWriter::create(_table, _cur_rowset, true);
    DCHECK(_writer != nullptr) << "memory error occur when creating writer";

    const std::vector<SlotDescriptor*>& slots = _req.tuple_desc->slots();
    for (auto& field_info : _table->tablet_schema()) {
        for (size_t i = 0; i < slots.size(); ++i) {
            if (slots[i]->col_name() == field_info.name) {
                _col_ids.push_back(i);
            }
        }
    }
    _field_infos = &(_table->tablet_schema());
    _schema = new Schema(*_field_infos),
    _mem_table = new MemTable(_schema, _field_infos, &_col_ids,
                              _req.tuple_desc, _table->keys_type());
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

        ++_rowset_id;
        _cur_rowset = new Rowset(_table.get(), false, _rowset_id, 0, true,
                                   _req.partition_id, _req.transaction_id);
        DCHECK(_cur_rowset != nullptr) << "failed to malloc Rowset";
        _cur_rowset->acquire();
        _cur_rowset->set_load_id(_req.load_id);
        _rowset_vec.push_back(_cur_rowset);

        SAFE_DELETE(_writer);
        _writer = IWriter::create(_table, _cur_rowset, true);
        DCHECK(_writer != nullptr) << "memory error occur when creating writer";

        SAFE_DELETE(_mem_table);
        _mem_table = new MemTable(_schema, _field_infos, &_col_ids,
                                  _req.tuple_desc, _table->keys_type());
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
    RETURN_NOT_OK(_table->add_pending_version(_req.partition_id, _req.transaction_id, nullptr));
    for (Rowset* rowset : _rowset_vec) {
        RETURN_NOT_OK(_table->add_pending_rowset(rowset));
        RETURN_NOT_OK(rowset->load());
    }
    if (_new_table != nullptr) {
        LOG(INFO) << "convert version for schema change";
        {
            MutexLock push_lock(_new_table->get_push_lock());
            // create pending data dir
            std::string dir_path = _new_table->construct_pending_data_dir_path();
            if (!check_dir_existed(dir_path)) {
                RETURN_NOT_OK(create_dirs(dir_path));
            }
        }
        SchemaChangeHandler schema_change;
        res = schema_change.schema_version_convert(
                    _table, _new_table, &_rowset_vec, &_new_rowset_vec);
        if (res != OLAP_SUCCESS) {
            LOG(WARNING) << "failed to convert delta for new table in schema change."
                << "res: " << res << ", " << "new_table: " << _new_table->full_name();
                return res;
        }

        RETURN_NOT_OK(_new_table->add_pending_version(_req.partition_id, _req.transaction_id, nullptr));
        for (Rowset* rowset : _new_rowset_vec) {
            RETURN_NOT_OK(_new_table->add_pending_rowset(rowset));
            RETURN_NOT_OK(rowset->load());
        }
    }

#ifndef BE_TEST
    PTabletInfo* tablet_info = tablet_vec->Add();
    tablet_info->set_tablet_id(_table->tablet_id());
    tablet_info->set_schema_hash(_table->schema_hash());
    if (_new_table != nullptr) {
        tablet_info = tablet_vec->Add();
        tablet_info->set_tablet_id(_new_table->tablet_id());
        tablet_info->set_schema_hash(_new_table->schema_hash());
    }
#endif

    _delta_written_success = true;
    return OLAP_SUCCESS;
}

OLAPStatus DeltaWriter::cancel() {
    DCHECK(!_is_init);
    return OLAP_SUCCESS;
}

}  // namespace palo
