#include <cstddef>
#include <string>
#include <gtest/gtest.h>
#include "vec/core/types.h"
#include "function_test_util.h"


namespace doris::vectorized {
TEST(FunctionMapConcatTest, TestBase) {
    const std::string func_name = "map_concat";
    {
        // simple case - two maps
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING
        };
        DataSet data_set = {
            {
                TestArray({
                    TestArray({std::int32_t(1), std::string("A")}),
                    TestArray({std::int32_t(2), std::string("B")})
                }),
                TestArray({
                    std::int32_t(1), std::string("A"),
                    std::int32_t(2), std::string("B")
                })
            }
        };
        
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed: " << status.to_string();
    }
    {
        // two maps concatenation with string keys
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_STRING
        };
        DataSet data_set = {
            {
                TestArray({
                    TestArray({std::string("A"), std::string("a")}),
                    TestArray({std::string("B"), std::string("b")})
                }),
                TestArray({
                    std::string("A"), std::string("a"),
                    std::string("B"), std::string("b")
                })
            }
        };
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed: " << status.to_string();
    }
    {
        // two maps concatenation with string keys
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_STRING
        };
        DataSet data_set = {
            {
                TestArray({
                    TestArray({std::string("A"), std::string("a")}),
                    TestArray({std::string("B"), std::string("b")})
                }),
                TestArray({
                    std::string("A"), std::string("a"),
                    std::string("B"), std::string("b")
                })
            },
            {
                TestArray({
                    TestArray({std::string("C"), std::string("c")}),
                    TestArray({std::string("D"), std::string("d")})
                }),
                TestArray({
                    std::string("C"), std::string("c"),
                    std::string("D"), std::string("d")
                })
            }
        };
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed: " << status.to_string();
    }
}

TEST(FunctionMapConcatTest, TestEdgeCases) {
    const std::string func_name = "map_concat";
    
    // Test empty maps
    {
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING
        };
        DataSet data_set = {
            {
                TestArray({TestArray({}), TestArray({})}),
                TestArray({})
            },
            {
                TestArray({TestArray({std::int32_t(1), std::string("A")}), TestArray({})}),
                TestArray({std::int32_t(1), std::string("A")})
            },
            {
                TestArray({TestArray({}), TestArray({std::int32_t(2), std::string("B")})}),
                TestArray({std::int32_t(2), std::string("B")})
            }
        };
        
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed one: " << status.to_string();
    }
    
    // Test key conflicts (later map should override earlier)
    {
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING
        };
        DataSet data_set = {
            {
                TestArray({
                    TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), std::string("B")}),
                    TestArray({std::int32_t(1), std::string("C"), std::int32_t(3), std::string("D")})
                }),
                TestArray({
                    std::int32_t(2), std::string("B"),
                    std::int32_t(1), std::string("C"),
                    std::int32_t(3), std::string("D")
                })
            }
        };
        
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed two: " << status.to_string();
    }
    
    // Test multiple maps (more than 2)
    {
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING
        };
        DataSet data_set = {
            {
                TestArray({
                    TestArray({std::int32_t(1), std::string("A")}),
                    TestArray({std::int32_t(2), std::string("B")}),
                    TestArray({std::int32_t(3), std::string("C")})
                }),
                TestArray({
                    std::int32_t(1), std::string("A"),
                    std::int32_t(2), std::string("B"),
                    std::int32_t(3), std::string("C")
                })
            }
        };
        
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed three: " << status.to_string();
    }
    
    // Test different value types
    {
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_INT,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_INT
        };
        DataSet data_set = {
            {
                TestArray({
                    TestArray({std::string("A"), std::int32_t(1), std::string("B"), std::int32_t(2)}),
                    TestArray({std::string("C"), std::int32_t(3), std::string("D"), std::int32_t(4)})
                }),
                TestArray({
                    std::string("A"), std::int32_t(1),
                    std::string("B"), std::int32_t(2),
                    std::string("C"), std::int32_t(3),
                    std::string("D"), std::int32_t(4)
                })
            }
        };
        
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed four: " << status.to_string();
    }
    
    
    // Test single map (should return the same map)
    {
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING
        };
        TestArray mp_src_array = TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), std::string("B")});
        TestArray src;
        src.push_back(mp_src_array);
        TestArray mp_dest_array = TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), std::string("B")});
        DataSet data_set = {
            {
                src,
                mp_dest_array
            }
        };
        any_cast<TestArray>(src[0]);
        // Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        // EXPECT_TRUE(status.ok()) << "Function test failed six : " << status.to_string();
    }
}

TEST(FunctionMapConcatTest, TestWithNULL){
    const std::string func_name = "map_concat";
    // Test with null map (one map is null)
    {
        // TODO cxr FIX Null
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING
        };
        DataSet data_set = {
            {
                TestArray({Null(), TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), std::string("B")})}),
                Null()
            },
            {
                TestArray({TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), std::string("B")}), Null()}),
                Null()
            },
            {
                TestArray({Null(), Null()}),
                Null()
            }
        };
        
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed seven: " << status.to_string();
    }

    // Test with null values
    {
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING
        };
        DataSet data_set = {
            {
                TestArray({
                    TestArray({std::int32_t(1), std::string("A"), std::int32_t(2), Null()}),
                    TestArray({std::int32_t(2), std::string("B"), std::int32_t(3), std::string("C")})
                }),
                TestArray({
                    std::int32_t(1), std::string("A"),
                    std::int32_t(2), std::string("B"),
                    std::int32_t(3), std::string("C")
                })
            }
        };
        
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed five: " << status.to_string();
    }
}

TEST(FunctionMapConcatTest, TestComplexTypes) {
    const std::string func_name = "map_concat";
    
    // // Test with nested types (array as value)
    // {
    //     InputTypeSet input_types = {
    //         PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT,
    //         PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_ARRAY, PrimitiveType::TYPE_INT, PrimitiveType::TYPE_INT
    //     };
    //     DataSet data_set = {
    //         {
    //             TestArray({
    //                 TestArray({
    //                     std::string("A"), 
    //                     TestArray({std::int32_t(1), std::int32_t(2)}),
    //                     std::string("B"),
    //                     std::int32_t(3)
    //                 }),
    //                 TestArray({
    //                     std::string("C"),
    //                     TestArray({std::int32_t(4), std::int32_t(5)})
    //                 })
    //             }),
    //             TestArray({
    //                 std::string("A"), TestArray({std::int32_t(1), std::int32_t(2)}),
    //                 std::string("B"), std::int32_t(3),
    //                 std::string("C"), TestArray({std::int32_t(4), std::int32_t(5)})
    //             })
    //         }
    //     };
        
    //     Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
    //     EXPECT_TRUE(status.ok()) << "Function test failed 1: " << status.to_string();
    // }
    
    // Test with map as value
    {
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_INT,
            PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_MAP, PrimitiveType::TYPE_STRING, PrimitiveType::TYPE_INT
        };
        DataSet data_set = {
            {
                TestArray({
                    TestArray({
                        std::string("outer1"),
                        TestArray({TestArray({std::string("inner1"), std::int32_t(1)})}),
                        std::string("outer2"),
                        TestArray({TestArray({std::string("inner2"), std::int32_t(2)})})
                    }),
                    TestArray({
                        std::string("outer3"),
                        TestArray({TestArray({std::string("inner3"), std::int32_t(3)})})
                    })
                }),
                TestArray({
                    std::string("outer1"), TestArray({TestArray({std::string("inner1"), std::int32_t(1)})}),
                    std::string("outer2"), TestArray({TestArray({std::string("inner2"), std::int32_t(2)})}),
                    std::string("outer3"), TestArray({TestArray({std::string("inner3"), std::int32_t(3)})})
                })
            }
        };
        
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed 2: " << status.to_string();
    }
}
}
