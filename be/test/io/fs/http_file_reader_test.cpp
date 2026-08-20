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

#include "io/fs/http_file_reader.h"

#include <gtest/gtest.h>

#include "io/file_factory.h"

namespace doris::io {

TEST(HttpFileReaderTest, ChunkResponseDisablesFileCache) {
    FileSystemProperties properties {
            .system_type = TFileType::FILE_HTTP,
            .properties = {{"http.enable.chunk.response", "true"}},
    };
    FileDescription file_description {.path = "http://127.0.0.1/stream"};
    FileReaderOptions opts {.cache_type = FileCachePolicy::FILE_BLOCK_CACHE};

    auto reader = FileFactory::create_file_reader(properties, file_description, opts);

    ASSERT_TRUE(reader.has_value()) << reader.error();
    EXPECT_NE(std::dynamic_pointer_cast<HttpFileReader>(reader.value()), nullptr);
}

} // namespace doris::io
