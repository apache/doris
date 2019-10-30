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

#include "tools/segment_tool.h"

#include <string>
#include <iostream>
#include <sstream>
#include <gflags/gflags.h>

#include "json2pb/pb_to_json.h"
#include "gutil/strings/substitute.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/binary_plain_page.h"

using doris::Status;
using doris::Slice;
using doris::RandomAccessFile;
using strings::Substitute;
using doris::segment_v2::SegmentFooterPB;
using doris::segment_v2::ColumnReader;
using doris::segment_v2::BinaryPlainPageDecoder;
using doris::segment_v2::PageHandle;
using doris::segment_v2::PagePointer;
using doris::segment_v2::ColumnReaderOptions;

DEFINE_string(file, "", "segment file path");
DEFINE_bool(show_meta, false, "show meta of segment file");
DEFINE_bool(show_dict, false, "show dict info of dict-encoded type");
DEFINE_int32(column_id, -1, "column to show");
DEFINE_int32(dict_num, -1, "dict items to show. -1: show nothing; 0: show all; >0: show dict_num items");

namespace doris {

void SegmentTool::process() {
    if (FLAGS_show_meta) {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show meta" << std::endl;
            return;
        }
        auto st = _show_meta(FLAGS_file);
        if (!st.ok()) {
            std::cout << "process show meta failed. Reason:" << st.to_string();
        }
        return;
    }
    if (FLAGS_show_dict) {
        if (FLAGS_file == "") {
            std::cout << "no file flag for show dict" << std::endl;
            return;
        }
        auto st = _show_dict(FLAGS_file);
        if (!st.ok()) {
            std::cout << "process show dict failed. Reason:" << st.to_string();
        }
        return;
    }

    std::cout << "print usage of segment tool" << std::endl;
}

Status SegmentTool::_show_meta(const std::string& file_name) {
    std::unique_ptr<RandomAccessFile> input_file;
    RETURN_IF_ERROR(doris::Env::Default()->new_random_access_file(file_name, &input_file));
    SegmentFooterPB footer;
    RETURN_IF_ERROR(_get_segment_footer(input_file.get(), &footer));
    std::string json_footer;
    json2pb::Pb2JsonOptions json_options;
    json_options.pretty_json = true;
    bool ret = json2pb::ProtoMessageToJson(footer, &json_footer, json_options);
    if (!ret) {
        return Status::Corruption("Convert PB to json failed");
    }
    std::cout << json_footer << std::endl;
    return Status::OK();
}

Status SegmentTool::_show_dict(const std::string& file_name) {
    std::unique_ptr<RandomAccessFile> input_file;
    RETURN_IF_ERROR(doris::Env::Default()->new_random_access_file(file_name, &input_file));
    SegmentFooterPB footer;
    RETURN_IF_ERROR(_get_segment_footer(input_file.get(), &footer));
    if (FLAGS_column_id < 0 || FLAGS_column_id >= footer.columns().size()) {
        return Status::InvalidArgument(strings::Substitute("invalid column id:$0, columns size:$1",
                FLAGS_column_id, footer.columns().size()));
    }
    ColumnReaderOptions opts;
    std::unique_ptr<ColumnReader> reader;
    RETURN_IF_ERROR(ColumnReader::create(opts, footer.columns(FLAGS_column_id),
                                         footer.num_rows(), input_file.get(),
                                         &reader));
    PagePointer dict_pp = reader->get_dict_page_pointer();
    PageHandle dict_page_handle;
    OlapReaderStatistics stats;
    RETURN_IF_ERROR(reader->read_page(dict_pp, &stats, &dict_page_handle));
    std::unique_ptr<BinaryPlainPageDecoder> dict_decoder;
    dict_decoder.reset(new BinaryPlainPageDecoder(dict_page_handle.data()));
    RETURN_IF_ERROR(dict_decoder->init());
    std::stringstream ss;
    ss << "dict info:" << std::endl;
    ss << "dict data size:" << dict_page_handle.data().size << ", dict item size:" << dict_decoder->count() << std::endl;
    if (FLAGS_dict_num >= 0) {
        ss << "dict items:";
        int dict_num = FLAGS_dict_num == 0 ? dict_decoder->count() : std::min(FLAGS_dict_num, (int32_t)dict_decoder->count());
        for (int i = 0; i < dict_num; ++i) {
            if (i % 10 == 0) {
                // every 10 items, start a new line
                ss << std::endl;
            }
            ss << " " << dict_decoder->string_at_index(i).to_string() << ",";
        }
        ss << std::endl;
    }
    std::cout << ss.str();
    return Status::OK();
}

Status SegmentTool::_get_segment_footer(RandomAccessFile* input_file, SegmentFooterPB* footer) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    std::string file_name = input_file->file_name();
    uint64_t file_size;
    RETURN_IF_ERROR(input_file->size(&file_size));

    if (file_size < 12) {
        return Status::Corruption(Substitute("Bad segment file $0: file size $1 < 12", file_name, file_size));
    }

    uint8_t fixed_buf[12];
    RETURN_IF_ERROR(input_file->read_at(file_size - 12, Slice(fixed_buf, 12)));

    // validate magic number
    const char* k_segment_magic = "D0R1";
    const uint32_t k_segment_magic_length = 4; 
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        return Status::Corruption(Substitute("Bad segment file $0: magic number not match", file_name));
    }

    // read footer PB
    uint32_t footer_length = doris::decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        return Status::Corruption(
            Substitute("Bad segment file $0: file size $1 < $2", file_name, file_size, 12 + footer_length));
    }
    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(input_file->read_at(file_size - 12 - footer_length, footer_buf));

    // validate footer PB's checksum
    uint32_t expect_checksum = doris::decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = doris::crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        return Status::Corruption(
            Substitute("Bad segment file $0: footer checksum not match, actual=$1 vs expect=$2",
                       file_name, actual_checksum, expect_checksum));
    }

    // deserialize footer PB
    if (!footer->ParseFromString(footer_buf)) {
        return Status::Corruption(Substitute("Bad segment file $0: failed to parse SegmentFooterPB", file_name));
    }
    return Status::OK();
}

}