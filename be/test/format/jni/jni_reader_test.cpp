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

#include "format/jni/jni_reader.h"

#include <gtest/gtest.h>

#include "util/jni_plugin_registry.h"

namespace doris {

TEST(JniReaderTest, PartialOpenDoesNotRunJavaFinalization) {
    // Java open must clean its own partial acquisition: legacy close skips unopened scanners.
    JniReader reader(Jni::PluginRef {.plugin = "unused", .factory = "unused"}, {});
    EXPECT_FALSE(reader._scanner_opened);
    // Leave _scanner_api null: any attempted Java cleanup would dereference it.
    EXPECT_TRUE(reader.close().ok());
    EXPECT_TRUE(reader._closed);
    EXPECT_TRUE(reader.close().ok());
}

} // namespace doris
