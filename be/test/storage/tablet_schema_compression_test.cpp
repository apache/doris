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

#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gtest/gtest.h>

#include "storage/tablet/tablet_meta.h"
#include "storage/tablet/tablet_schema.h"

namespace doris {

TEST(TabletColumnCompressionTest, InitFromPbRoundTrip) {
    ColumnPB pb;
    pb.set_unique_id(1);
    pb.set_name("c1");
    pb.set_type("INT");
    pb.set_compression_type(segment_v2::ZSTD);
    pb.set_compression_level(9);

    TabletColumn col;
    col.init_from_pb(pb);
    ASSERT_TRUE(col.has_compression());
    ASSERT_EQ(col.compression(), segment_v2::ZSTD);
    ASSERT_EQ(col.compression_level(), 9);

    ColumnPB out;
    col.to_schema_pb(&out);
    ASSERT_TRUE(out.has_compression_type());
    ASSERT_EQ(out.compression_type(), segment_v2::ZSTD);
    ASSERT_EQ(out.compression_level(), 9);
}

TEST(TabletColumnCompressionTest, InitFromPbNoOverride) {
    ColumnPB pb;
    pb.set_unique_id(1);
    pb.set_name("c1");
    pb.set_type("INT");
    TabletColumn col;
    col.init_from_pb(pb);
    ASSERT_FALSE(col.has_compression());
    ASSERT_EQ(col.compression(), segment_v2::UNKNOWN_COMPRESSION);

    ColumnPB out;
    col.to_schema_pb(&out);
    ASSERT_FALSE(out.has_compression_type());
}

TEST(TabletColumnCompressionTest, InitFromThriftRoundTrip) {
    TColumn tcolumn;
    tcolumn.column_name = "c1";
    tcolumn.column_type.type = TPrimitiveType::INT;
    tcolumn.__set_is_key(true);
    tcolumn.__set_compression_type(static_cast<int32_t>(TCompressionType::ZSTD));
    tcolumn.__set_compression_level(9);

    TabletColumn col;
    col.init_from_thrift(tcolumn);
    ASSERT_TRUE(col.has_compression());
    ASSERT_EQ(col.compression(), segment_v2::ZSTD);
    ASSERT_EQ(col.compression_level(), 9);

    ColumnPB out;
    col.to_schema_pb(&out);
    ASSERT_TRUE(out.has_compression_type());
    ASSERT_EQ(out.compression_type(), segment_v2::ZSTD);
    ASSERT_EQ(out.compression_level(), 9);
}

TEST(TabletColumnCompressionTest, InitFromThriftNoOverride) {
    TColumn tcolumn;
    tcolumn.column_name = "c1";
    tcolumn.column_type.type = TPrimitiveType::INT;
    tcolumn.__set_is_key(true);

    TabletColumn col;
    col.init_from_thrift(tcolumn);
    ASSERT_FALSE(col.has_compression());

    ColumnPB out;
    col.to_schema_pb(&out);
    ASSERT_FALSE(out.has_compression_type());
}

// The persisted (non-cloud) tablet meta is built by init_column_from_tcolumn, not by
// TabletColumn::init_from_thrift. Compaction reloads that persisted ColumnPB, so if the
// compression override is dropped here it silently reverts to the table default after the
// first compaction. Assert the persisted ColumnPB carries the per-column codec.
TEST(TabletColumnCompressionTest, InitColumnFromTColumnPersistsCompression) {
    TColumn tcolumn;
    tcolumn.column_name = "c1";
    tcolumn.column_type.type = TPrimitiveType::INT;
    tcolumn.__set_is_key(true);
    tcolumn.__set_compression_type(static_cast<int32_t>(TCompressionType::ZSTD));
    tcolumn.__set_compression_level(9);

    ColumnPB column;
    TabletMeta::init_column_from_tcolumn(1, tcolumn, &column);
    ASSERT_TRUE(column.has_compression_type());
    ASSERT_EQ(column.compression_type(), segment_v2::ZSTD);
    ASSERT_EQ(column.compression_level(), 9);
}

// A codec without an explicit level must persist the type but leave the level unset
// (level absent => codec default).
TEST(TabletColumnCompressionTest, InitColumnFromTColumnPersistsCompressionNoLevel) {
    TColumn tcolumn;
    tcolumn.column_name = "c1";
    tcolumn.column_type.type = TPrimitiveType::INT;
    tcolumn.__set_is_key(true);
    tcolumn.__set_compression_type(static_cast<int32_t>(TCompressionType::LZ4F));

    ColumnPB column;
    TabletMeta::init_column_from_tcolumn(1, tcolumn, &column);
    ASSERT_TRUE(column.has_compression_type());
    ASSERT_EQ(column.compression_type(), segment_v2::LZ4F);
    ASSERT_FALSE(column.has_compression_level());
}

TEST(TabletColumnCompressionTest, InitColumnFromTColumnNoOverride) {
    TColumn tcolumn;
    tcolumn.column_name = "c1";
    tcolumn.column_type.type = TPrimitiveType::INT;
    tcolumn.__set_is_key(true);

    ColumnPB column;
    TabletMeta::init_column_from_tcolumn(1, tcolumn, &column);
    ASSERT_FALSE(column.has_compression_type());
    ASSERT_EQ(column.compression_type(), segment_v2::UNKNOWN_COMPRESSION);
    ASSERT_FALSE(column.has_compression_level());
}

} // namespace doris
