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

#include "format_v2/orc/orc_search_argument.h"

#include <gtest/gtest.h>

#include <memory>
#include <numeric>
#include <orc/OrcFile.hh>
#include <orc/sargs/SearchArgument.hh>
#include <string>
#include <tuple>
#include <vector>

#include "core/data_type/data_type_nullable.h"
#include "core/data_type/data_type_number.h"
#include "core/field.h"
#include "exprs/vectorized_fn_call.h"
#include "exprs/vin_predicate.h"
#include "exprs/vliteral.h"
#include "exprs/vslot_ref.h"
#include "format/orc/orc_memory_stream_test.h"

namespace doris {
namespace {

class OrcSearchArgumentSignedZeroTest : public testing::TestWithParam<std::tuple<bool, bool>> {
protected:
    static constexpr int64_t ROWS_PER_GROUP = 1000;

    void SetUp() override {
        const auto [use_float, multiple_stripes] = GetParam();
        _value_type = use_float ? DataTypePtr(std::make_shared<DataTypeFloat32>())
                                : DataTypePtr(std::make_shared<DataTypeFloat64>());
        _type = ::orc::Type::buildTypeFromString(use_float ? "struct<id:bigint,value:float>"
                                                           : "struct<id:bigint,value:double>");
        MemoryOutputStream output(1024 * 1024);
        ::orc::WriterOptions options;
        options.setRowIndexStride(ROWS_PER_GROUP)
                .setStripeSize(multiple_stripes ? 1 : 1024 * 1024)
                .setColumnsUseBloomFilter({2})
                .setBloomFilterFPP(0.000001)
                .setCompression(::orc::CompressionKind_NONE);
        auto writer = ::orc::createWriter(*_type, &output, options);
        auto batch = writer->createRowBatch(ROWS_PER_GROUP);
        auto& root = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& ids = dynamic_cast<::orc::LongVectorBatch&>(*root.fields[0]);
        auto& values = dynamic_cast<::orc::DoubleVectorBatch&>(*root.fields[1]);
        for (int64_t group = 0; group < 4; ++group) {
            root.numElements = ids.numElements = values.numElements = ROWS_PER_GROUP;
            values.hasNulls = group == 3;
            for (int64_t row = 0; row < ROWS_PER_GROUP; ++row) {
                ids.data[row] = group * ROWS_PER_GROUP + row;
                values.notNull[row] = group != 3;
                // Separate signs into different Bloom filters; min/max alone keeps both zeros.
                if (group == 0) {
                    values.data[row] = -0.0;
                } else if (group == 1) {
                    values.data[row] = 0.0;
                } else {
                    values.data[row] = row % 2 ? 1.0 : 3.0;
                }
            }
            writer->add(*batch);
        }
        writer->close();
        _file.assign(output.getData(), output.getLength());
        auto reader = create_orc_reader();
        ASSERT_EQ(reader->getNumberOfStripes(), multiple_stripes ? 4 : 1);
    }

    std::unique_ptr<::orc::Reader> create_orc_reader() const {
        return ::orc::createReader(std::make_unique<MemoryInputStream>(_file.data(), _file.size()),
                                   ::orc::ReaderOptions());
    }

    VExprSPtr literal(double value) const {
        const auto field = std::get<0>(GetParam())
                                   ? Field::create_field<TYPE_FLOAT>(static_cast<float>(value))
                                   : Field::create_field<TYPE_DOUBLE>(value);
        return VLiteral::create_shared(_value_type, field);
    }

    VExprSPtr predicate(TExprOpcode::type op, std::vector<VExprSPtr> values,
                        bool literal_on_left = false) const {
        const bool is_in = op == TExprOpcode::FILTER_IN;
        TExprNode node;
        auto node_type = TExprNodeType::BINARY_PRED;
        if (is_in) {
            node_type = TExprNodeType::IN_PRED;
        } else if (op == TExprOpcode::EQ_FOR_NULL) {
            node_type = TExprNodeType::NULL_AWARE_BINARY_PRED;
        }
        node.__set_node_type(node_type);
        node.__set_opcode(op);
        node.__set_type(make_nullable(std::make_shared<DataTypeUInt8>())->to_thrift());
        node.__set_is_nullable(true);
        node.in_predicate.__set_is_not_in(false);
        VExprSPtr expr = is_in ? VExprSPtr(VInPredicate::create_shared(node))
                               : VExprSPtr(VectorizedFnCall::create_shared(node));
        auto slot = VSlotRef::create_shared(1, 1, -1, make_nullable(_value_type), "value");
        if (literal_on_left) {
            expr->add_child(values.front());
            expr->add_child(slot);
        } else {
            expr->add_child(slot);
            for (const auto& value : values) {
                expr->add_child(value);
            }
        }
        return expr;
    }

    std::vector<int64_t> selected_ids(const VExprSPtr& expr) const {
        format::FileScanRequest request;
        request.local_positions.emplace(format::LocalColumnId(1), format::LocalIndex(1));
        auto builder = ::orc::SearchArgumentFactory::newBuilder();
        const bool pushed = format::orc::build_orc_search_argument(
                request, *_type, cctz::utc_time_zone(), {}, expr, builder);
        EXPECT_TRUE(pushed);
        if (!pushed) {
            return {};
        }
        ::orc::RowReaderOptions options;
        options.searchArgument(builder->build());
        auto reader = create_orc_reader();
        auto row_reader = reader->createRowReader(options);
        auto batch = row_reader->createRowBatch(512);
        auto& root = dynamic_cast<::orc::StructVectorBatch&>(*batch);
        auto& ids = dynamic_cast<::orc::LongVectorBatch&>(*root.fields[0]);
        std::vector<int64_t> result;
        while (row_reader->next(*batch)) {
            for (uint64_t row = 0; row < batch->numElements; ++row) {
                result.push_back(ids.data[row]);
            }
        }
        return result;
    }

    static std::vector<int64_t> group_ids(int64_t first, int64_t count) {
        std::vector<int64_t> ids(count * ROWS_PER_GROUP);
        std::iota(ids.begin(), ids.end(), first * ROWS_PER_GROUP);
        return ids;
    }

    DataTypePtr _value_type;
    std::unique_ptr<::orc::Type> _type;
    std::string _file;
};

TEST_P(OrcSearchArgumentSignedZeroTest, EqualityKeepsBothSigns) {
    for (double zero : {0.0, -0.0}) {
        for (bool literal_on_left : {false, true}) {
            EXPECT_EQ(selected_ids(predicate(TExprOpcode::EQ, {literal(zero)}, literal_on_left)),
                      group_ids(0, 2));
        }
    }
}

TEST_P(OrcSearchArgumentSignedZeroTest, NullSafeEqualityKeepsBothSigns) {
    for (double zero : {0.0, -0.0}) {
        for (bool literal_on_left : {false, true}) {
            EXPECT_EQ(selected_ids(predicate(TExprOpcode::EQ_FOR_NULL, {literal(zero)},
                                             literal_on_left)),
                      group_ids(0, 2));
        }
    }
}

TEST_P(OrcSearchArgumentSignedZeroTest, InKeepsBothSigns) {
    for (double zero : {0.0, -0.0}) {
        EXPECT_EQ(selected_ids(predicate(TExprOpcode::FILTER_IN, {literal(zero)})),
                  group_ids(0, 2));
        EXPECT_EQ(selected_ids(predicate(TExprOpcode::FILTER_IN, {literal(2.0), literal(zero)})),
                  group_ids(0, 2));
        EXPECT_EQ(selected_ids(predicate(TExprOpcode::FILTER_IN,
                                         {literal(zero), literal(-zero), literal(2.0)})),
                  group_ids(0, 2));
        auto null_literal = VLiteral::create_shared(make_nullable(_value_type), Field());
        EXPECT_EQ(selected_ids(predicate(TExprOpcode::FILTER_IN, {literal(zero), null_literal})),
                  group_ids(0, 2));
    }
}

TEST_P(OrcSearchArgumentSignedZeroTest, NullSafeNullStillSelectsOnlyNullGroup) {
    auto null_literal = VLiteral::create_shared(make_nullable(_value_type), Field());
    EXPECT_EQ(selected_ids(predicate(TExprOpcode::EQ_FOR_NULL, {null_literal})), group_ids(3, 1));
}

TEST_P(OrcSearchArgumentSignedZeroTest, NonzeroBloomPruningIsPreserved) {
    // 2 is inside the [1, 3] bounds, so only Bloom can reject this nonzero candidate.
    EXPECT_TRUE(selected_ids(predicate(TExprOpcode::EQ, {literal(2.0)})).empty());
    EXPECT_TRUE(
            selected_ids(predicate(TExprOpcode::FILTER_IN, {literal(2.0), literal(2.5)})).empty());
    EXPECT_EQ(selected_ids(predicate(TExprOpcode::EQ, {literal(1.0)})), group_ids(2, 1));
    EXPECT_EQ(selected_ids(predicate(TExprOpcode::LE, {literal(0.0)})), group_ids(0, 2));
}

INSTANTIATE_TEST_SUITE_P(FloatAndDoubleRowGroupsAndStripes, OrcSearchArgumentSignedZeroTest,
                         testing::Combine(testing::Bool(), testing::Bool()));

} // namespace
} // namespace doris
