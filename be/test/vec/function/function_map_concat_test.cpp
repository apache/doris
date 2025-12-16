#include <gtest/gtest.h>

#include <cstddef>
#include <string>

#include "function_test_util.h"
#include "vec/core/types.h"

namespace doris::vectorized {
TEST(FunctionMapConcatTest, TestBase) {
    const std::string func_name = "map_concat";
    {
        // simple case - two maps
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_INT,    PrimitiveType::TYPE_STRING};
        DataSet data_set = {{TestArray({TestArray({std::int32_t(1), std::string("A")}),
                                        TestArray({std::int32_t(2), std::string("B")})}),
                             TestArray({std::int32_t(1), std::string("A"), std::int32_t(2),
                                        std::string("B")})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
        // EXPECT_TRUE(status.ok()) << "Function test failed: " << status.to_string();
    }
    {
        // two maps concatenation with string keys
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_STRING};
        DataSet data_set = {{TestArray({TestArray({std::string("A"), std::string("a")}),
                                        TestArray({std::string("B"), std::string("b")})}),
                             TestArray({std::string("A"), std::string("a"), std::string("B"),
                                        std::string("b")})}};
        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }
    {
        // two maps concatenation with string keys
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_STRING};
        DataSet data_set = {{TestArray({TestArray({std::string("A"), std::string("a")}),
                                        TestArray({std::string("B"), std::string("b")})}),
                             TestArray({std::string("A"), std::string("a"), std::string("B"),
                                        std::string("b")})},
                            {TestArray({TestArray({std::string("C"), std::string("c")}),
                                        TestArray({std::string("D"), std::string("d")})}),
                             TestArray({std::string("C"), std::string("c"), std::string("D"),
                                        std::string("d")})}};
        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }
}

TEST(FunctionMapConcatTest, TestEdgeCases) {
    const std::string func_name = "map_concat";

    // Test empty maps
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_INT,    PrimitiveType::TYPE_STRING};
        DataSet data_set = {
                {TestArray({TestArray({}), TestArray({})}), TestArray({})},
                {TestArray({TestArray({std::int32_t(1), std::string("A")}), TestArray({})}),
                 TestArray({std::int32_t(1), std::string("A")})},
                {TestArray({TestArray({}), TestArray({std::int32_t(2), std::string("B")})}),
                 TestArray({std::int32_t(2), std::string("B")})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }

    // Test key conflicts (later map should override earlier)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_INT,    PrimitiveType::TYPE_STRING};
        DataSet data_set = {{TestArray({TestArray({std::int32_t(1), std::string("A"),
                                                   std::int32_t(2), std::string("B")}),
                                        TestArray({std::int32_t(1), std::string("C"),
                                                   std::int32_t(3), std::string("D")})}),
                             TestArray({std::int32_t(2), std::string("B"), std::int32_t(1),
                                        std::string("C"), std::int32_t(3), std::string("D")})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }

    // Test multiple maps (more than 2)
    {
        InputTypeSet input_types = {
                PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
                PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
                PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING};
        DataSet data_set = {{TestArray({TestArray({std::int32_t(1), std::string("A")}),
                                        TestArray({std::int32_t(2), std::string("B")}),
                                        TestArray({std::int32_t(3), std::string("C")})}),
                             TestArray({std::int32_t(1), std::string("A"), std::int32_t(2),
                                        std::string("B"), std::int32_t(3), std::string("C")})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }

    // Test different value types
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_INT,    PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_INT};
        DataSet data_set = {{TestArray({TestArray({std::string("A"), std::int32_t(1),
                                                   std::string("B"), std::int32_t(2)}),
                                        TestArray({std::string("C"), std::int32_t(3),
                                                   std::string("D"), std::int32_t(4)})}),
                             TestArray({std::string("A"), std::int32_t(1), std::string("B"),
                                        std::int32_t(2), std::string("C"), std::int32_t(3),
                                        std::string("D"), std::int32_t(4)})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }

    // Test single map (should return the same map)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_STRING};
        TestArray mp_src_array =
                TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), std::string("B")});
        TestArray src;
        src.push_back(mp_src_array);
        TestArray mp_dest_array =
                TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), std::string("B")});
        DataSet data_set = {{src, mp_dest_array}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }
}

TEST(FunctionMapConcatTest, TestWithNULL) {
    const std::string func_name = "map_concat";
    // Test with null map (one map is null)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_INT,    PrimitiveType::TYPE_STRING};
        DataSet data_set = {{TestArray({Null(), TestArray({std::int32_t(1), std::string("A"),
                                                           std::int32_t(2), std::string("B")})}),
                             Null()},
                            {TestArray({TestArray({std::int32_t(1), std::string("A"),
                                                   std::int32_t(2), std::string("B")}),
                                        Null()}),
                             Null()},
                            {TestArray({Null(), Null()}), Null()}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }

    // Test with null values
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_INT,    PrimitiveType::TYPE_STRING};
        DataSet data_set = {
                {TestArray({TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), Null()}),
                            TestArray({std::int32_t(2), std::string("B"), std::int32_t(3),
                                       std::string("C")})}),
                 TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), std::string("B"),
                            std::int32_t(3), std::string("C")})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }
}

TEST(FunctionMapConcatTest, TestComplexTypes) {
    const std::string func_name = "map_concat";

    // Test with nested types (array as value)
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,   PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT,
                                    PrimitiveType::TYPE_MAP,   PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT};
        DataSet data_set = {
                {TestArray({TestArray({std::string("A"),
                                       TestArray({std::int32_t(1), std::int32_t(2)}),
                                       std::string("B"),
                                       TestArray({std::int32_t(3), std::int32_t(5)})}),
                            TestArray({std::string("C"),
                                       TestArray({std::int32_t(4), std::int32_t(5)})})}),
                 TestArray({std::string("A"), TestArray({std::int32_t(1), std::int32_t(2)}),
                            std::string("B"), TestArray({std::int32_t(3), std::int32_t(5)}),
                            std::string("C"), TestArray({std::int32_t(4), std::int32_t(5)})})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }

    // Test with map as value
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_INT,    PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_INT};
        DataSet data_set = {
                {TestArray(
                         {TestArray(
                                  {std::string("outer1"),
                                   TestArray({TestArray({std::string("inner1"), std::int32_t(1)})}),
                                   std::string("outer2"),
                                   TestArray(
                                           {TestArray({std::string("inner2"), std::int32_t(2)})})}),
                          TestArray({std::string("outer3"),
                                     TestArray({TestArray(
                                             {std::string("inner3"), std::int32_t(3)})})})}),
                 TestArray({std::string("outer1"),
                            TestArray({TestArray({std::string("inner1"), std::int32_t(1)})}),
                            std::string("outer2"),
                            TestArray({TestArray({std::string("inner2"), std::int32_t(2)})}),
                            std::string("outer3"),
                            TestArray({TestArray({std::string("inner3"), std::int32_t(3)})})})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }

    // Test with double as value
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_DOUBLE, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_DOUBLE};
        DataSet data_set = {
                {TestArray({TestArray({std::string("key1"), 1.5, std::string("key2"), 2.7}),
                            TestArray({std::string("key3"), 3.9})}),
                 TestArray({std::string("key1"), 1.5, std::string("key2"), 2.7, std::string("key3"),
                            3.9})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }

    // Test with float as value
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,    PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_FLOAT,  PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_FLOAT};
        DataSet data_set = {
                {TestArray({TestArray({std::string("key1"), 1.5f, std::string("key2"), 2.7f}),
                            TestArray({std::string("key3"), 3.9f})}),
                 TestArray({std::string("key1"), 1.5f, std::string("key2"), 2.7f,
                            std::string("key3"), 3.9f})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }

    // Test with decimalv2 as value
    {
        InputTypeSet input_types = {PrimitiveType::TYPE_MAP,       PrimitiveType::TYPE_STRING,
                                    PrimitiveType::TYPE_DECIMALV2, PrimitiveType::TYPE_MAP,
                                    PrimitiveType::TYPE_STRING,    PrimitiveType::TYPE_DECIMALV2};
        DataSet data_set = {{TestArray({TestArray({std::string("key1"), ut_type::DECIMALV2(1.5),
                                                   std::string("key2"), ut_type::DECIMALV2(2.7)}),
                                        TestArray({std::string("key3"), ut_type::DECIMALV2(3.9)})}),
                             TestArray({std::string("key1"), ut_type::DECIMALV2(1.5),
                                        std::string("key2"), ut_type::DECIMALV2(2.7),
                                        std::string("key3"), ut_type::DECIMALV2(3.9)})}};

        check_function_all_arg_comb<DataTypeMap, true>(func_name, input_types, data_set);
    }
}
} // namespace doris::vectorized
