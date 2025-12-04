#include <cstddef>
#include <string>
#include <gtest/gtest.h>
#include "vec/core/types.h"
#include "function_test_util.h"


namespace doris::vectorized {
TEST(FunctionMapConcatTest, TestBase) {
    const std::string func_name = "map_concat";
    {
        InputTypeSet input_types = {
            PrimitiveType::TYPE_MAP,PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING,
            PrimitiveType::TYPE_MAP,PrimitiveType::TYPE_INT, PrimitiveType::TYPE_STRING
        };
        DataSet data_set = {
            {
                TestArray({TestArray({std::int32_t(1),std::string("A")}),TestArray({std::int32_t(2),std::string("B")})}),
                TestArray({std::int32_t(1),std::string("A"),std::int32_t(2),std::string("B")})
            }
        };
        
        // // 处理 check_function 的返回值
        Status status = check_function<DataTypeMap, true>(func_name, input_types, data_set);
        EXPECT_TRUE(status.ok()) << "Function test failed: " << status.to_string();
    }
}
}
