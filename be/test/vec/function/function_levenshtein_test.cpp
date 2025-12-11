#include <gtest/gtest.h>
#include <string>
#include <vector>
#include "function_test_util.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"

using namespace doris;
using namespace doris::vectorized;

TEST(function_string_test, function_levenshtein_comprehensive_test) {
    std::string func_name = "levenshtein";
    
    InputTypeSet input_types = {PrimitiveType::TYPE_VARCHAR, PrimitiveType::TYPE_VARCHAR};

    DataSet data_set = {
        // === 1. 经典教科书案例 ===
        {{std::string("kitten"), std::string("sitting")}, (int32_t)3},
        {{std::string("saturday"), std::string("sunday")}, (int32_t)3},
        {{std::string("rosettacode"), std::string("raisethysword")}, (int32_t)8},
        
        // === 2. 基础增删改测试 ===
        // 完全相等
        {{std::string("test"), std::string("test")}, (int32_t)0},
        // 替换 (Substitution)
        {{std::string("cat"), std::string("bat")}, (int32_t)1},
        {{std::string("abc"), std::string("xyz")}, (int32_t)3},
        // 插入 (Insertion)
        {{std::string("book"), std::string("books")}, (int32_t)1},
        {{std::string("test"), std::string("mytest")}, (int32_t)2},
        // 删除 (Deletion)
        {{std::string("apple"), std::string("app")}, (int32_t)2},
        
        // === 3. 边界条件 (空字符串) ===
        {{std::string(""), std::string("")}, (int32_t)0},
        {{std::string("a"), std::string("")}, (int32_t)1},
        {{std::string(""), std::string("abc")}, (int32_t)3},
        
        // === 4. 大小写敏感性 (Levenshtein 通常区分大小写) ===
        {{std::string("A"), std::string("a")}, (int32_t)1}, // 需要一次替换
        {{std::string("Doris"), std::string("doris")}, (int32_t)1},
        
        // === 5. 特殊字符与长字符串 ===
        {{std::string("a-b-c"), std::string("a_b_c")}, (int32_t)2}, // 替换两个符号
        {{std::string("1234567890"), std::string("1234567890")}, (int32_t)0},
        
        // === 6. 中文/多字节字符测试 ===
        // 注意：Doris 的 StringRef 是按字节(Byte)存储的，Levenshtein 算法也是按字节计算的。
        // UTF-8 编码中，常用汉字占 3 个字节。
        // "中" (3 bytes) vs "中" (3 bytes) -> 距离 0
        {{std::string("中"), std::string("中")}, (int32_t)0},
        // "中国" (6 bytes) vs "中" (3 bytes) -> 距离 3 (删掉"国"字的3个字节)
        {{std::string("中国"), std::string("中")}, (int32_t)3}, 
        // "你好" (6 bytes) vs "您好" (6 bytes) -> 距离 3 (把"你"的3字节变成"您"的3字节，需要3次编辑)
        // 或者是先删3个再加3个，取决于算法实现细节，通常 DP 结果是 3 (3次替换字节)
        {{std::string("你好"), std::string("您好")}, (int32_t)3},
        
        // === 7. NULL 值测试 (框架会自动处理，但为了保险起见手动测一下组合) ===
        // 左边 NULL
        {{Null(), std::string("abc")}, Null()},
        // 右边 NULL
        {{std::string("abc"), Null()}, Null()},
        // 两边 NULL
        {{Null(), Null()}, Null()}
    };
    
    check_function_all_arg_comb<DataTypeInt32, true>(func_name, input_types, data_set);
}
