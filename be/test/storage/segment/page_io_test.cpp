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

#include "storage/segment/page_io.h"

#include <gen_cpp/segment_v2.pb.h>
#include <gtest/gtest.h>

#include <algorithm>
#include <string>
#include <vector>

#include "util/block_compression.h"
#include "util/faststring.h"
#include "util/slice.h"

namespace doris::segment_v2 {

TEST(PageIOTest, ZstdCompressedPageShrinksRetainedCapacity) {
    BlockCompressionCodec* codec = nullptr;
    ASSERT_TRUE(get_block_compression_codec(CompressionTypePB::ZSTD, &codec).ok());

    std::string raw_page(9 * 1024 * 1024, 'a');
    std::vector<Slice> body {Slice(raw_page)};
    OwnedSlice compressed_body;
    ASSERT_TRUE(PageIO::compress_page_body(codec, 0, body, &compressed_body).ok());
    ASSERT_FALSE(compressed_body.slice().empty());

    EXPECT_LE(compressed_body.capacity(),
              std::max(compressed_body.slice().size,
                       static_cast<size_t>(faststring::kInitialCapacity)));

    std::string decompressed(raw_page.size(), '\0');
    Slice decompressed_slice(decompressed);
    ASSERT_TRUE(codec->decompress(compressed_body.slice(), &decompressed_slice).ok());
    EXPECT_EQ(raw_page, decompressed);
}

} // namespace doris::segment_v2
