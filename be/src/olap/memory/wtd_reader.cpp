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

#include "olap/memory/wtd_reader.h"

#include "olap/fs/fs_util.h"
#include "olap/memory/wtd_memtable.h"
#include "util/crc32c.h"

namespace doris {
namespace memory {

WTDReader::WTDReader(fs::ReadableBlock* rblock) : _rblock(rblock) {
    CHECK(_rblock != nullptr);
}

Status WTDReader::open() {
    RETURN_IF_ERROR(_parse_footer());
    return Status::OK();
}

std::shared_ptr<PartialRowBatch> WTDReader::build_batch(scoped_refptr<Schema>* schema) {
    // TODO: Schema may be changed? May need check footer.
    auto batch = std::make_shared<PartialRowBatch>(schema);

    // Load WriteTxnData all at once, data_size & num_rows have been set in _parse_footer()
    // TODO: not necessarily all, can be split by size or row
    std::vector<uint8_t> buffer(sizeof(uint64_t) + _data_size);
    CHECK(_footer.has_num_rows());
    *reinterpret_cast<uint64_t*>(buffer.data()) = _footer.num_rows();
    auto data_pos = buffer.data() + sizeof(uint64_t);
    _rblock->read(0, Slice(data_pos, _data_size));
    batch->load(std::move(buffer));

    return batch;
}

Status WTDReader::_parse_footer() {
    uint64_t file_size = 0;
    RETURN_IF_ERROR(_rblock->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(
                Substitute("Bad WTD file $0: file size $1 < 12", _wtd_file_path, file_size));
    }

    // Footer := FileFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(_rblock->read(file_size - 12, Slice(fixed_buf, 12)));

    // validate magic number
    if (memcmp(fixed_buf + 8, k_wtd_magic, k_wtd_magic_length) != 0) {
        return Status::Corruption(
                Substitute("Bad WTD file $0: magic number not match", _wtd_file_path));
    }

    // read footer PB
    uint32_t footer_length = decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption(Substitute("Bad WTD file $0: file size $1 < $2", _wtd_file_path,
                                             file_size, 12 + footer_length));
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);

    RETURN_IF_ERROR(_rblock->read(file_size - 12 - footer_length, footer_buf));

    // validate footer PB's checksum
    uint32_t expect_checksum = decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
                Substitute("Bad WTD file $0: footer checksum not match, actual=$1 vs expect=$2",
                           _wtd_file_path, actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    if (!_footer.ParseFromString(footer_buf)) {
        return Status::Corruption(
                Substitute("Bad WTD file $0: failed to parse SegmentFooterPB", _wtd_file_path));
    }
    CHECK(_footer.version() == 1);

    // get data size
    _data_size = file_size - 12 - footer_length;

    return Status::OK();
}

} // namespace memory
} // namespace doris