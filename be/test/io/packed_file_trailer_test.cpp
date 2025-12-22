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

#include "io/fs/packed_file_trailer.h"

#include <gtest/gtest.h>
#include <unistd.h>

#include <filesystem>
#include <fstream>
#include <string>

#include "common/status.h"
#include "util/coding.h"

namespace doris::io {

using doris::Status;

namespace {
std::string unique_temp_file() {
    auto dir = std::filesystem::temp_directory_path();
    std::string pattern = (dir / "packed_file_XXXXXX").string();
    int fd = mkstemp(pattern.data());
    if (fd != -1) {
        close(fd);
    }
    return pattern;
}

void write_file(const std::string& path, const std::string& data) {
    std::ofstream ofs(path, std::ios::binary | std::ios::trunc);
    ofs.write(data.data(), data.size());
}
} // namespace

TEST(PackedFileTrailerTest, ReadNewFormatTrailer) {
    cloud::PackedFileInfoPB info;
    info.set_resource_id("resource");
    info.set_total_slice_num(1);
    auto* slice = info.add_slices();
    slice->set_path("a");
    slice->set_offset(10);
    slice->set_size(20);

    cloud::PackedFileFooterPB footer_pb;
    footer_pb.mutable_packed_file_info()->CopyFrom(info);

    std::string serialized_footer;
    ASSERT_TRUE(footer_pb.SerializeToString(&serialized_footer));

    std::string file_content = "data";
    file_content.append(serialized_footer);
    put_fixed32_le(&file_content, static_cast<uint32_t>(serialized_footer.size()));
    put_fixed32_le(&file_content, kPackedFileTrailerVersion);

    auto path = unique_temp_file();
    write_file(path, file_content);

    cloud::PackedFileFooterPB parsed;
    uint32_t version = 0;
    Status st = read_packed_file_trailer(path, &parsed, &version);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(version, kPackedFileTrailerVersion);
    ASSERT_TRUE(parsed.has_packed_file_info());
    EXPECT_EQ(parsed.packed_file_info().resource_id(), info.resource_id());
    ASSERT_EQ(parsed.packed_file_info().slices_size(), 1);
    EXPECT_EQ(parsed.packed_file_info().slices(0).path(), "a");
    EXPECT_EQ(parsed.packed_file_info().slices(0).offset(), 10);
    EXPECT_EQ(parsed.packed_file_info().slices(0).size(), 20);

    std::filesystem::remove(path);
}

TEST(PackedFileTrailerTest, ReadLegacyTrailer) {
    cloud::PackedFileInfoPB info;
    info.set_total_slice_num(2);
    info.set_remaining_slice_bytes(100);

    std::string serialized_info;
    ASSERT_TRUE(info.SerializeToString(&serialized_info));

    std::string file_content = "legacy_payload";
    file_content.append(serialized_info);
    put_fixed32_le(&file_content, static_cast<uint32_t>(serialized_info.size()));

    auto path = unique_temp_file();
    write_file(path, file_content);

    cloud::PackedFileFooterPB parsed;
    uint32_t version = 0;
    Status st = read_packed_file_trailer(path, &parsed, &version);
    ASSERT_TRUE(st.ok()) << st;
    EXPECT_EQ(version, 0);
    ASSERT_TRUE(parsed.has_packed_file_info());
    EXPECT_EQ(parsed.packed_file_info().total_slice_num(), info.total_slice_num());
    EXPECT_EQ(parsed.packed_file_info().remaining_slice_bytes(), info.remaining_slice_bytes());

    std::filesystem::remove(path);
}

} // namespace doris::io
