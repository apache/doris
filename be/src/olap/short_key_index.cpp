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

#include "olap/short_key_index.h"

#include <string>

#include "util/coding.h"
#include "gutil/strings/substitute.h"

using strings::Substitute;

namespace doris {

Status ShortKeyIndexBuilder::init() {
    _key_length = 0;
    for (auto& key : _key_fields) {
        // +1 for null bit, we should improve this some day
        _key_length += key.index_length + 1;
    }
    _tmp_key_buf.resize(_key_length);
    return Status::OK();
}

Status ShortKeyIndexBuilder::add_item(const RowCursor& short_key, uint32_t block_id) {
    char* key_buf = (char*)_tmp_key_buf.data();

    size_t offset = 0;
    for (size_t i = 0; i < _key_fields.size(); i++) {
        short_key.write_index_by_index(i, key_buf + offset);
        offset += short_key.get_index_size(i) + 1;
    }

    _index_buf.append(key_buf, _key_length);
    put_fixed32_le(&_index_buf, block_id);

    return Status::OK();
}

Status ShortKeyIndexBuilder::finalize(uint32_t data_length,
                                      uint32_t num_rows,
                                      std::vector<Slice>* slices) {
    // extra fixed part
    _header.mutable_extra()->data_length = data_length;
    _header.mutable_extra()->num_rows = num_rows;

    // proto part
    _header.mutable_message()->set_start_version(0);
    _header.mutable_message()->set_end_version(0);
    _header.mutable_message()->set_cumulative_version_hash(0);
    _header.mutable_message()->set_segment(_segment_id);
    _header.mutable_message()->set_num_rows_per_block(_num_rows_per_block);
    _header.mutable_message()->set_delete_flag(_delete_flag);
    _header.mutable_message()->set_null_supported(true);

    std::string proto_string;
    if (!_header.message().SerializeToString(&proto_string)) {
        return Status::InternalError("Serialize short key header failed");
    }

    // fixed part
    _header._fixed_file_header.magic_number = OLAP_FIX_HEADER_MAGIC_NUMBER;
    _header._fixed_file_header.version = OLAP_DATA_VERSION_APPLIED;

    _header._fixed_file_header.protobuf_length = proto_string.size();
    _header._fixed_file_header.protobuf_checksum =
        olap_adler32(ADLER32_INIT, proto_string.data(), proto_string.size());

    // NOTE: must set file_length after protobuf_length has been set, because
    // _header.size() depends on protobuf_length
    _header._fixed_file_header.file_length = _index_buf.size() + _header.size();
    _header._fixed_file_header.checksum = 
        olap_adler32(ADLER32_INIT, (const char*)_index_buf.data(), _index_buf.size());

    _header_buf.resize(_header.size());
    char* ptr = (char*)_header_buf.data();

    memcpy(ptr, &_header._fixed_file_header, sizeof(_header._fixed_file_header));
    ptr += sizeof(_header._fixed_file_header);

    memcpy(ptr, &_header._extra_fixed_header, sizeof(_header._extra_fixed_header));
    ptr += sizeof(_header._extra_fixed_header);

    memcpy(ptr, proto_string.data(), proto_string.size());

    slices->emplace_back(_header_buf);
    slices->emplace_back(_index_buf);
    return Status::OK();
}

Status ShortKeyIndexDecoder::parse() {
    Slice data = _data;

    // parse fix
    auto fixed_length = _header._fixed_file_header_size;
    if (data.size < fixed_length) {
        return Status::Corruption("Header length is too small");
    }
    memcpy(&_header._fixed_file_header, data.data, fixed_length);

    // check magic
    if (_header._fixed_file_header.magic_number != OLAP_FIX_HEADER_MAGIC_NUMBER) {
        return Status::Corruption(
            Substitute("Magic not match, magic=$0", _header._fixed_file_header.magic_number));
    }
    data.remove_prefix(fixed_length);

    // parse extra
    auto extra_size = sizeof(_header._extra_fixed_header);
    if (data.size < extra_size) {
        return Status::Corruption("Header length is too small");
    }
    memcpy(&_header._extra_fixed_header, data.data, extra_size);
    data.remove_prefix(extra_size);

    auto proto_length = _header._fixed_file_header.protobuf_length;
    if (data.size < proto_length) {
        return Status::Corruption(
            Substitute("Header is too small to load proto expect=$0, real=$1",
                       proto_length, data.size));
    }
    // check proto checksum
    uint32_t proto_checksum = olap_adler32(ADLER32_INIT, data.data, proto_length);
    if (proto_checksum != _header._fixed_file_header.protobuf_checksum) {
        return Status::Corruption(
            Substitute("Protobuf checksum don't match expect=$0, real=$1",
                       _header._fixed_file_header.protobuf_checksum, proto_checksum));
    }

    std::string proto_buf(data.data, proto_length);
    if (!_header._proto.ParseFromString(proto_buf)) {
        return Status::Corruption("Failed to parse protobuf in header");
    }
    data.remove_prefix(proto_length);
    _index_data = data;
    return Status::OK();
}

}
