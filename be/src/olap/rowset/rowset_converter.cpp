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

#include "olap/rowset/rowset_converter.h"

#include "olap/row_block.h"
#include "olap/row_block2.h"
#include "olap/rowset/rowset_factory.h"

namespace doris {

OLAPStatus RowsetConverter::convert_beta_to_alpha(const RowsetMetaSharedPtr& src_rowset_meta,
                                                  const FilePathDesc& rowset_path_desc,
                                                  RowsetMetaPB* dst_rs_meta_pb) {
    return _convert_rowset(src_rowset_meta, rowset_path_desc, ALPHA_ROWSET, dst_rs_meta_pb);
}

OLAPStatus RowsetConverter::convert_alpha_to_beta(const RowsetMetaSharedPtr& src_rowset_meta,
                                                  const FilePathDesc& rowset_path_desc,
                                                  RowsetMetaPB* dst_rs_meta_pb) {
    return _convert_rowset(src_rowset_meta, rowset_path_desc, BETA_ROWSET, dst_rs_meta_pb);
}

OLAPStatus RowsetConverter::_convert_rowset(const RowsetMetaSharedPtr& src_rowset_meta,
                                            const FilePathDesc& rowset_path_desc, RowsetTypePB dst_type,
                                            RowsetMetaPB* dst_rs_meta_pb) {
    const TabletSchema& tablet_schema = _tablet_meta->tablet_schema();
    RowsetWriterContext context;
    context.rowset_id = src_rowset_meta->rowset_id();
    context.tablet_uid = _tablet_meta->tablet_uid();
    context.tablet_id = _tablet_meta->tablet_id();
    context.partition_id = _tablet_meta->partition_id();
    context.tablet_schema_hash = _tablet_meta->schema_hash();
    context.rowset_type = dst_type;
    context.path_desc = rowset_path_desc;
    context.tablet_schema = &tablet_schema;
    context.rowset_state = src_rowset_meta->rowset_state();
    context.segments_overlap = src_rowset_meta->segments_overlap();
    if (context.rowset_state == VISIBLE) {
        context.version = src_rowset_meta->version();
    } else {
        context.txn_id = src_rowset_meta->txn_id();
        context.load_id = src_rowset_meta->load_id();
    }
    std::unique_ptr<RowsetWriter> rowset_writer;
    RETURN_NOT_OK(RowsetFactory::create_rowset_writer(context, &rowset_writer));
    if (!src_rowset_meta->empty()) {
        RowsetSharedPtr rowset;
        RETURN_NOT_OK(RowsetFactory::create_rowset(&tablet_schema, rowset_path_desc, src_rowset_meta,
                                                   &rowset));
        RowsetReaderSharedPtr rowset_reader;
        RETURN_NOT_OK(rowset->create_reader(&rowset_reader));
        std::vector<uint32_t> cids;
        for (int i = 0; i < tablet_schema.num_columns(); ++i) {
            cids.push_back(i);
        }
        DeleteHandler delete_handler;
        RowsetReaderContext reader_context;
        reader_context.reader_type = READER_ALTER_TABLE;
        reader_context.tablet_schema = &tablet_schema;
        reader_context.return_columns = &cids;
        reader_context.seek_columns = &cids;
        reader_context.delete_handler = &delete_handler;
        reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
        RETURN_NOT_OK(rowset_reader->init(&reader_context));
        // convert
        RowBlock* row_block = nullptr;
        RowCursor row_cursor;
        row_cursor.init(tablet_schema);
        while (true) {
            if (row_block == nullptr || !row_block->has_remaining()) {
                auto st = rowset_reader->next_block(&row_block);
                if (st != OLAP_SUCCESS) {
                    if (st == OLAP_ERR_DATA_EOF) {
                        break;
                    } else {
                        return st;
                    }
                }
            }
            while (row_block->has_remaining()) {
                size_t pos = row_block->pos();
                row_block->get_row(pos, &row_cursor);
                RETURN_NOT_OK(rowset_writer->add_row(row_cursor));
                row_block->pos_inc();
            }
        }
    }
    RETURN_NOT_OK(rowset_writer->flush());
    RowsetSharedPtr dst_rowset = rowset_writer->build();
    if (dst_rowset == nullptr) {
        return OLAP_ERR_MALLOC_ERROR;
    }
    if (src_rowset_meta->has_delete_predicate()) {
        // should set the delete predicate to the rowset meta
        dst_rowset->rowset_meta()->set_delete_predicate(src_rowset_meta->delete_predicate());
    }
    dst_rowset->rowset_meta()->to_rowset_pb(dst_rs_meta_pb);
    return OLAP_SUCCESS;
}

} // namespace doris
