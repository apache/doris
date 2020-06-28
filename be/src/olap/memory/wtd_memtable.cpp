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

#include "olap/memory/wtd_memtable.h"

#include "gen_cpp/segment_v2.pb.h"
#include "gen_cpp/wtd_file.pb.h"
#include "util/crc32c.h"
#include "util/faststring.h"

namespace doris {
namespace memory {

const char* k_wtd_magic = "WTD1";
const uint32_t k_wtd_magic_length = 4;

WTDMemTable::WTDMemTable(const doris::Schema* schema, fs::WritableBlock* wblock)
        : _schema(schema), _wblock(wblock) {
    CHECK(_schema != nullptr);
    CHECK(_wblock != nullptr);
}

void WTDMemTable::insert(const void* data) {
    // append(_buffer, row, sizeof(uint32_t)+*(uint32_t*)row)
    auto pos = reinterpret_cast<const uint8_t*>(data);
    auto array_size = sizeof(uint32_t) + (*reinterpret_cast<const uint32_t*>(pos));
    _buffer.insert(_buffer.end(), pos, pos + array_size);
    _num_rows++;
}

OLAPStatus WTDMemTable::flush() {
    // footer will save the default values of non-key cols in schema part(ColumnMetaPB).
    // Use the default values for this write txn, otherwise use the default values
    // in tablet schema.

    // generate footer
    wtd_file::FileFooterPB footer;
    footer.set_version(1);
    uint32_t column_id = 0;
    for (auto& cid : _schema->column_ids()) {
        auto field = _schema->column(cid);

        auto column_meta = footer.add_columns();
        // TODO(zc): Do we need this column_id??
        column_meta->set_column_id(column_id++);
        column_meta->set_unique_id(cid);
        column_meta->set_type(field->type());
        column_meta->set_length(field->length());
        column_meta->set_encoding(segment_v2::EncodingTypePB::DEFAULT_ENCODING);
        column_meta->set_compression(segment_v2::CompressionTypePB::LZ4F);
        column_meta->set_is_nullable(field->is_nullable());
    }
    footer.set_num_rows(_num_rows);

    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string footer_buf;
    if (!footer.SerializeToString(&footer_buf)) {
        LOG(WARNING) << "failed to serialize WTD footer";
        return OLAP_ERR_INIT_FAILED;
    }

    faststring fixed_buf;
    // footer's size
    put_fixed32_le(&fixed_buf, footer_buf.size());
    // footer's checksum
    uint32_t checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    put_fixed32_le(&fixed_buf, checksum);
    // Append magic number. we don't write magic number in the header because
    // that will need an extra seek when reading
    fixed_buf.append(k_wtd_magic, k_wtd_magic_length);

    // append(_buffer, footer) & write_to_disk(_buffer)
    std::vector<Slice> slices{Slice(_buffer.data(), _buffer.size()), footer_buf, fixed_buf};

    auto st = _wblock->appendv(&slices[0], slices.size());
    if (!st.ok()) {
        LOG(WARNING) << "WTD file write failed. path=" << _wblock->path();
        return OLAPStatus::OLAP_ERR_IO_ERROR;
    }

    // won't return error
    _wblock->finalize();
    _buffer.clear();

    return OLAPStatus::OLAP_SUCCESS;
}

} // namespace memory
} // namespace doris