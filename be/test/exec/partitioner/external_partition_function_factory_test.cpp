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
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/partitioner/external/external_partition_function_factory.h"

#include <gtest/gtest.h>

#include <map>
#include <memory>
#include <string>
#include <utility>

#include "runtime/descriptor_helper.h"

namespace doris {
namespace {

TExpr int_slot_ref_expr() {
    TExprNode node;
    node.__set_node_type(TExprNodeType::SLOT_REF);
    node.__set_num_children(0);
    node.__set_type(create_type_desc(TYPE_INT));
    node.__set_is_nullable(false);
    TSlotRef slot_ref;
    slot_ref.__set_slot_id(1);
    slot_ref.__set_tuple_id(1);
    node.__set_slot_ref(slot_ref);
    TExpr expression;
    expression.nodes.emplace_back(std::move(node));
    return expression;
}

} // namespace

TEST(ExternalPartitionFunctionFactoryTest, CreatesGenericDirectHash) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_partition_function("direct_hash");
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::IDENTITY);
    std::unique_ptr<PartitionFunction> function;

    ASSERT_TRUE(create_external_partition_function(info, 3, ShuffleHashMethod::CRC32, {}, &function)
                        .ok());
    ASSERT_NE(function, nullptr);
    EXPECT_EQ(function->partition_count(), 3);
}

TEST(ExternalPartitionFunctionFactoryTest, RejectsUnknownFunction) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_partition_function("connector_owned_function");
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::IDENTITY);
    std::unique_ptr<PartitionFunction> function;

    Status status =
            create_external_partition_function(info, 3, ShuffleHashMethod::CRC32, {}, &function);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("Unsupported external sink partition function"),
              std::string::npos);
}

TEST(ExternalPartitionFunctionFactoryTest, DirectHashRejectsOpaqueOptions) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_partition_function("direct_hash");
    info.__set_partition_function_options({{"unexpected", "value"}});
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::IDENTITY);
    std::unique_ptr<PartitionFunction> function;

    Status status =
            create_external_partition_function(info, 3, ShuffleHashMethod::CRC32, {}, &function);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("does not accept options"), std::string::npos);
}

TEST(ExternalPartitionFunctionFactoryTest, PaimonFixedBucketValidatesOptions) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_partition_function("paimon_fixed_bucket");
    info.__set_partition_function_options({{"num_buckets", "4"},
                                           {"partition_field_indexes", ""},
                                           {"bucket_field_indexes", "0"},
                                           {"unexpected", "value"}});
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::IDENTITY);
    std::unique_ptr<PartitionFunction> function;

    Status status =
            create_external_partition_function(info, 3, ShuffleHashMethod::CRC32, {}, &function);
    EXPECT_FALSE(status.ok());
    EXPECT_NE(status.to_string().find("requires num_buckets"), std::string::npos);
}

TEST(ExternalPartitionFunctionFactoryTest, ReadsBranch41DirectHashEncoding) {
    TExternalTableSinkHashPartitionInfo info;
    info.__set_algorithm(TExternalTableSinkHashAlgorithm::DIRECT_HASH);
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::SKEWED);
    std::unique_ptr<PartitionFunction> function;

    ASSERT_TRUE(create_external_partition_function(info, 3, ShuffleHashMethod::CRC32, {}, &function)
                        .ok());
    ASSERT_NE(function, nullptr);
    EXPECT_EQ(function->partition_count(), 3);
}

TEST(ExternalPartitionFunctionFactoryTest, ReadsBranch41PaimonFixedBucketEncoding) {
    TPaimonFixedBucketInfo fixed_bucket_info;
    fixed_bucket_info.__set_num_buckets(4);
    fixed_bucket_info.__set_partition_field_indexes({});
    fixed_bucket_info.__set_bucket_field_indexes({0});
    TExternalTableSinkHashPartitionInfo info;
    info.__set_algorithm(TExternalTableSinkHashAlgorithm::PAIMON_FIXED_BUCKET);
    info.__set_writer_assignment(TExternalTableSinkWriterAssignment::IDENTITY);
    info.__set_paimon_fixed_bucket_info(fixed_bucket_info);
    std::unique_ptr<PartitionFunction> function;

    ASSERT_TRUE(create_external_partition_function(info, 3, ShuffleHashMethod::CRC32,
                                                   {int_slot_ref_expr()}, &function)
                        .ok());
    ASSERT_NE(function, nullptr);
    EXPECT_EQ(function->partition_count(), 3);
}

} // namespace doris
