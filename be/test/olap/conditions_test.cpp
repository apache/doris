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

#include <gtest/gtest.h>

#include "olap/olap_cond.h"
#include "olap/tablet_schema_helper.h"

namespace doris {

class CondTest : public testing::Test {
public:
    CondTest() {
        _tablet_column = create_int_key(_col);
    }

    void SetUp() override {
        CpuInfo::init();
    }

    Cond create_cond(const std::string& op, const std::vector<std::string>& operands) {
        TCondition tcond;
        tcond.__set_column_name(std::to_string(_col));
        tcond.__set_condition_op(op);
        tcond.__set_condition_values(operands);

        Cond cond;
        cond.init(tcond, _tablet_column);

        return cond;
    }

    void check_operand(Cond cond, CondOp op, const std::string& operand) {
        if (op == OP_ALL) {
            ASSERT_EQ(cond.operand_field, nullptr);
            ASSERT_EQ(cond.operand_set.size(), 0);
            ASSERT_EQ(cond.min_value_field, nullptr);
            ASSERT_EQ(cond.max_value_field, nullptr);
        } else if (op == OP_IS) {
            ASSERT_NE(cond.operand_field, nullptr);
            ASSERT_EQ(operand == "NULL", cond.operand_field->is_null());
        } else {
            ASSERT_NE(cond.operand_field, nullptr);
            ASSERT_EQ(operand, cond.operand_field->to_string());
        }
    }

    void check_operands(Cond cond, const std::set<std::string>& operands) {
        ASSERT_EQ(cond.operand_field, nullptr);
        ASSERT_NE(cond.min_value_field, nullptr);
        ASSERT_NE(cond.max_value_field, nullptr);
        ASSERT_EQ(cond.min_value_field->to_string(), *(operands.begin()));
        ASSERT_EQ(cond.max_value_field->to_string(), *(operands.rbegin()));
        ASSERT_EQ(cond.operand_set.size(), operands.size());
        for (const auto& operand : cond.operand_set) {
            ASSERT_EQ(operands.count(operand->to_string()), 1);
        }
    }

    void calc_and_check(bool is_intersection,
                        Cond cond1, Cond cond2, CondOp op,
                        const std::string& operand = "",
                        const std::set<std::string>& operands = {}) {
        if (is_intersection) {
            ASSERT_EQ(cond1.intersection_cond(cond2), OLAP_SUCCESS);
        } else {
            ASSERT_EQ(cond1.union_cond(cond2), OLAP_SUCCESS);
        }
        ASSERT_EQ(cond1.op, op);
        if (op != OP_NULL) {
            if (op == OP_IN || op == OP_NOT_IN) {
                check_operands(cond1, operands);
            } else {
                check_operand(cond1, op, operand);
            }
        }
    }

private:
    static const int32_t _col = 0;
    TabletColumn _tablet_column;
};

TEST_F(CondTest, InitTest) {
}

TEST_F(CondTest, IntersectionCondsTest) {
    struct {
        std::string op1;
        std::vector<std::string> operands1;
        std::string op2;
        std::vector<std::string> operands2;
        CondOp result_op;
        std::string result_operand;
        std::set<std::string> result_operands;
    } test_cases[] = {{"=",   {"1"},           "=",   {"1"},           OP_EQ,     "1",        {}},
                      {"=",   {"1"},           "=",   {"2"},           OP_NULL,   "",         {}},
                      {"=",   {"1"},           "*=",  {"1", "2", "3"}, OP_EQ,     "1",        {}},
                      {"=",   {"1"},           "*=",  {"2", "3"},      OP_NULL,   "",         {}},

                      {"!=",  {"1"},           "!=",  {"1"},           OP_NE,     "1",        {}},
                      {"!=",  {"1"},           "!=",  {"2"},           OP_NOT_IN, "",         {"1", "2"}},
                      {"!=",  {"1"},           "!*=", {"1"},           OP_NE,     "1",        {}},
                      {"!=",  {"1"},           "!*=", {"1", "2", "3"}, OP_NOT_IN, "",         {"1", "2", "3"}},
                      {"!=",  {"1"},           "!*=", {"2", "3"},      OP_NOT_IN, "",         {"1", "2", "3"}},
                      {"!=",  {"3"},           "!*=", {"1", "2"},      OP_NOT_IN, "",         {"1", "2", "3"}},

                      {"<",   {"1"},           "<",   {"1"},           OP_LT,     "1",        {}},
                      {"<",   {"1"},           "<",   {"2"},           OP_LT,     "1",        {}},
                      {"<",   {"2"},           "<",   {"1"},           OP_LT,     "1",        {}},
                      {"<",   {"1"},           "<=",  {"1"},           OP_LT,     "1",        {}},

                      {"<=",  {"1"},           "<=",  {"1"},           OP_LE,     "1",        {}},
                      {"<=",  {"1"},           "<=",  {"2"},           OP_LE,     "1",        {}},
                      {"<=",  {"2"},           "<=",  {"1"},           OP_LE,     "1",        {}},
                      {"<=",  {"1"},           "<",   {"1"},           OP_LT,     "1",        {}},

                      {">",   {"1"},           ">",   {"1"},           OP_GT,     "1",        {}},
                      {">",   {"1"},           ">",   {"2"},           OP_GT,     "2",        {}},
                      {">",   {"2"},           ">",   {"1"},           OP_GT,     "2",        {}},
                      {">",   {"1"},           ">=",  {"1"},           OP_GT,     "1",        {}},

                      {">=",  {"1"},           ">=",  {"1"},           OP_GE,     "1",        {}},
                      {">=",  {"1"},           ">=",  {"2"},           OP_GE,     "2",        {}},
                      {">=",  {"2"},           ">=",  {"1"},           OP_GE,     "2",        {}},
                      {">=",  {"1"},           ">",   {"1"},           OP_GT,     "1",        {}},

                      {"*=",  {"1"},           "*=",  {"2"},           OP_NULL,   "",         {}},
                      {"*=",  {"1", "2", "3"}, "*=",  {"2"},           OP_EQ,     "2",        {}},
                      {"*=",  {"1", "2", "3"}, "*=",  {"2", "3"},      OP_IN,     "",         {"2", "3"}},
                      {"*=",  {"1"},           "=",   {"2"},           OP_NULL,   "",         {}},
                      {"*=",  {"1", "2"},      "=",   {"1"},           OP_EQ,     "1",        {}},

                      {"!*=", {"1", "2"},      "!*=", {"1", "3"},      OP_NOT_IN, "",         {"1", "2", "3"}},
                      {"!*=", {"1", "2", "3"}, "!=",  {"2"},           OP_NOT_IN, "",         {"1", "2", "3"}},
                      {"!*=", {"1", "2"},      "!=",  {"3"},           OP_NOT_IN, "",         {"1", "2", "3"}},

                      {"is",  {"NULL"},        "is",  {"NOT NULL"},    OP_NULL,   "",         {}},
                      {"is",  {"NOT NULL"},    "is",  {"NULL"},        OP_NULL,   "",         {}},
                      {"is",  {"NULL"},        "is",  {"NULL"},        OP_IS,     "NULL",     {}},
                      {"is",  {"NOT NULL"},    "is",  {"NOT NULL"},    OP_IS,     "NOT NULL", {}}};

    int i = 0;
    for (const auto &test : test_cases) {
        ASSERT_NO_FATAL_FAILURE(calc_and_check(true,
                                               create_cond(test.op1, test.operands1),
                                               create_cond(test.op2, test.operands2),
                                               test.result_op,
                                               test.result_operand,
                                               test.result_operands)) << "error index: " << i;
        ++i;
    }
}

TEST_F(CondTest, UnionCondsTest) {
    struct {
        std::string op1;
        std::vector<std::string> operands1;
        std::string op2;
        std::vector<std::string> operands2;
        CondOp result_op;
        std::string result_operand;
        std::set<std::string> result_operands;
    } test_cases[] = {{"=",   {"1"},           "=",   {"1"},           OP_EQ,     "1",        {}},
                      {"=",   {"1"},           "=",   {"2"},           OP_IN,     "",         {"1", "2"}},
                      {"=",   {"1"},           "*=",  {"1", "2"},      OP_IN,     "",         {"1", "2"}},
                      {"=",   {"1"},           "*=",  {"2", "3"},      OP_IN,     "",         {"1", "2", "3"}},

                      {"!=",  {"1"},           "!=",  {"1"},           OP_NE,     "1",        {}},
                      {"!=",  {"1"},           "!=",  {"2"},           OP_ALL,    "",         {}},
                      {"!=",  {"1"},           "!*=", {"2", "3"},      OP_ALL,    "",         {}},
                      {"!=",  {"1"},           "!*=", {"1", "2"},      OP_NE,     "1",        {}},

                      {"<",   {"1"},           "<",   {"1"},           OP_LT,     "1",        {}},
                      {"<",   {"1"},           "<",   {"2"},           OP_LT,     "2",        {}},
                      {"<",   {"2"},           "<",   {"1"},           OP_LT,     "2",        {}},
                      {"<",   {"1"},           "<=",  {"1"},           OP_LE,     "1",        {}},

                      {"<=",  {"1"},           "<=",  {"1"},           OP_LE,     "1",        {}},
                      {"<=",  {"1"},           "<=",  {"2"},           OP_LE,     "2",        {}},
                      {"<=",  {"2"},           "<=",  {"1"},           OP_LE,     "2",        {}},
                      {"<=",  {"1"},           "<",   {"1"},           OP_LE,     "1",        {}},

                      {">",   {"1"},           ">",   {"1"},           OP_GT,     "1",        {}},
                      {">",   {"1"},           ">",   {"2"},           OP_GT,     "1",        {}},
                      {">",   {"2"},           ">",   {"1"},           OP_GT,     "1",        {}},
                      {">",   {"1"},           ">=",  {"1"},           OP_GE,     "1",        {}},

                      {">=",  {"1"},           ">=",  {"1"},           OP_GE,     "1",        {}},
                      {">=",  {"1"},           ">=",  {"2"},           OP_GE,     "1",        {}},
                      {">=",  {"2"},           ">=",  {"1"},           OP_GE,     "1",        {}},
                      {">=",  {"1"},           ">",   {"1"},           OP_GE,     "1",        {}},

                      {"*=",  {"1"},           "*=",  {"2"},           OP_IN,     "",         {"1", "2"}},
                      {"*=",  {"1", "2", "3"}, "*=",  {"2", "4"},      OP_IN,     "",         {"1", "2", "3", "4"}},
                      {"*=",  {"1", "2", "3"}, "=",   {"2"},           OP_IN,     "",         {"1", "2", "3"}},
                      {"*=",  {"2", "3"},      "=",   {"1"},           OP_IN,     "",         {"1", "2", "3"}},
                      {"*=",  {"1", "2"},      "=",   {"3"},           OP_IN,     "",         {"1", "2", "3"}},

                      {"!*=", {"1"},           "!*=", {"2"},           OP_ALL,    "",         {}},
                      {"!*=", {"1", "2", "3"}, "!*=", {"2", "4"},      OP_NE,     "2",        {}},
                      {"!*=", {"1", "2", "3"}, "!*=", {"2", "3", "4"}, OP_NOT_IN, "",         {"2", "3"}},
                      {"!*=", {"1", "2"},      "!=",  {"3"},           OP_ALL,    "",         {}},
                      {"!*=", {"1", "2"},      "!=",  {"1"},           OP_NE,     "1",        {}},

                      {"is",  {"NULL"},        "is",  {"NOT NULL"},    OP_ALL,    "",         {}},
                      {"is",  {"NOT NULL"},    "is",  {"NULL"},        OP_ALL,    "",         {}},
                      {"is",  {"NULL"},        "is",  {"NULL"},        OP_IS,     "NULL",     {}},
                      {"is",  {"NOT NULL"},    "is",  {"NOT NULL"},    OP_IS,     "NOT NULL", {}}};
    int i = 0;
    for (const auto &test : test_cases) {
        ASSERT_NO_FATAL_FAILURE(calc_and_check(false,
                                               create_cond(test.op1, test.operands1),
                                               create_cond(test.op2, test.operands2),
                                               test.result_op,
                                               test.result_operand,
                                               test.result_operands)) << "error index: " << i;
        ++i;
    }
}

}  // namespace doris

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
