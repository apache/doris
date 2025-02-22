#include <gtest/gtest.h>
#include "runtime/primitive_type.h"
#include "vec/functions/string_utils.h"
#include "testutil/column_helper.h"          // ColumnHelper 用于构造列和 Block
#include "vec/data_types/data_type_string.h"
#include "vec/data_types/data_type_number.h"
#include "vec/core/block.h"
#include <string>
#include <vector>

namespace doris::vectorized {


// 测试 fixture
class StringUtilsTest : public ::testing::Test {
protected:
    // 用于测试 push 系列函数的内部数据结构
    ColumnString::Chars chars;
    ColumnString::Offsets offsets;
    NullMap null_map;

    void SetUp() override {
        chars.clear();
        offsets.clear();
        null_map.clear();
    }

    // 辅助方法：利用 ColumnHelper 创建字符串类型 Block
    Block create_string_block(const std::vector<std::string>& data) {
        return ColumnHelper::create_block<DataTypeString>(data);
    }

    // 辅助方法：利用 ColumnHelper 创建整数类型 Block
    Block create_int_block(const std::vector<int32_t>& data) {
        return ColumnHelper::create_block<DataTypeInt32>(data);
    }
};

// 测试 push_empty_string：写入空字符串不增加 chars 内容，但更新 offsets
TEST_F(StringUtilsTest, TestPushEmptyString) {
    // 假设 row_index 表示当前需要写入的行号
    size_t row_index = 0;
    // 调用函数
    StringOP::push_empty_string(row_index, chars, offsets);
    // 检查：offsets 应新增一个值，且值等于当前 chars 大小
    ASSERT_EQ(offsets.size(), 1);
    ASSERT_EQ(offsets[0], chars.size());
    // 空字符串不应增加任何字符
    ASSERT_EQ(chars.size(), 0);
}

// 测试 push_null_string：写入 null 值同时更新 null_map
TEST_F(StringUtilsTest, TestPushNullString) {
    size_t row_index = 0;
    StringOP::push_null_string(row_index, chars, offsets, null_map);
    // offsets 增加一个值，等于当前 chars 大小
    ASSERT_EQ(offsets.size(), 1);
    ASSERT_EQ(offsets[0], chars.size());
    // null_map 应标记该行为 null（假设使用 1 表示 null）
    ASSERT_EQ(null_map.size(), 1);
    ASSERT_EQ(null_map[0], 1);
}

// 测试 push_value_string：写入一个非空字符串
TEST_F(StringUtilsTest, TestPushValueString) {
    std::string_view test_str = "test";
    size_t row_index = 0;
    StringOP::push_value_string(test_str, row_index, chars, offsets);
    // offsets 应新增一个元素，其值应等于写入后的 chars 大小
    ASSERT_EQ(offsets.size(), 1);
    ASSERT_EQ(offsets[0], chars.size());
    // chars 大小应等于字符串长度
    size_t expected_size = test_str.size();
    ASSERT_EQ(chars.size(), expected_size);
    // 将写入的内容转换为 std::string 进行比较
    std::string result(reinterpret_cast<const char*>(chars.data()), chars.size());
    ASSERT_EQ(result, std::string(test_str));
}

// 测试 push_value_string_reserved_and_allow_overflow：功能与 push_value_string 类似
TEST_F(StringUtilsTest, TestPushValueStringReservedAndAllowOverflow) {
    std::string_view test_str = "overflow";
    size_t row_index = 0;
    StringOP::push_value_string_reserved_and_allow_overflow(test_str, row_index, chars, offsets);
    ASSERT_EQ(offsets.size(), 1);
    ASSERT_EQ(offsets[0], chars.size());
    size_t expected_size = test_str.size();
    ASSERT_EQ(chars.size(), expected_size);
    std::string result(reinterpret_cast<const char*>(chars.data()), chars.size());
    ASSERT_EQ(result, std::string(test_str));
}

// 测试 fast_repeat：将源数组重复多次复制到目标缓冲区
TEST_F(StringUtilsTest, TestFastRepeat) {
    // 源字符数组 "abc"，重复 3 次，目标结果应为 "abcabcabc"
    uint8_t src[] = {'a', 'b', 'c'};
    const size_t src_size = 3;
    const size_t repeat_times = 3;
    const size_t dst_size = src_size * repeat_times;
    std::vector<uint8_t> dst(dst_size, 0);
    StringOP::fast_repeat(dst.data(), src, src_size, repeat_times);
    std::string expected = "abcabcabc";
    std::string result(reinterpret_cast<const char*>(dst.data()), dst_size);
    ASSERT_EQ(result, expected);
}

// 测试 SubstringUtil::substring_execute：对 Block 中的字符串进行截取操作
TEST_F(StringUtilsTest, TestSubstringExecute) {
    // 输入数据：字符串、起始位置、截取长度
    std::vector<std::string> input_strings = {"abcdefg", "hello world", "doris"};
    std::vector<int32_t> starts = {2, 1, 3};   // 假设使用 1 索引，即 1 表示第一个字符
    std::vector<int32_t> lengths = {3, 5, 2};

    // 预期输出：
    // "abcdefg", start=2, length=3 -> "bcd"
    // "hello world", start=1, length=5 -> "hello"
    // "doris", start=3, length=2 -> "ri"
    std::vector<std::string> expected_results = {"bcd", "hello", "ri"};

    // 构造输入 Block，利用 ColumnHelper 创建各列
    Block block;
    ColumnWithTypeAndName col_strings = ColumnHelper::create_column_with_name<DataTypeString>(input_strings);
    ColumnWithTypeAndName col_starts = ColumnHelper::create_column_with_name<DataTypeInt32>(starts);
    ColumnWithTypeAndName col_lengths = ColumnHelper::create_column_with_name<DataTypeInt32>(lengths);
    block.insert(col_strings);
    block.insert(col_starts);
    block.insert(col_lengths);

    // 构造一个空的结果列（DataTypeString），供 substring_execute 写入截取结果
    auto result_column = DataTypeString().create_column();
    auto data_type_str = std::make_shared<DataTypeString>();
    ColumnWithTypeAndName col_result(result_column, data_type_str, "result");
    block.insert(col_result);

    // 参数配置：arguments 列号 {0, 1, 2} 分别为字符串、起始位置和长度，结果列索引为 3
    ColumnNumbers arguments = {0, 1, 2};
    uint32_t result_index = 3;
    size_t input_rows_count = input_strings.size();

    // 调用截取函数，执行截取操作
    SubstringUtil::substring_execute(block, arguments, result_index, input_rows_count);

    // 逐行检查结果列中的输出是否与预期一致
    auto result_col = block.get_by_position(result_index).column;
    for (size_t i = 0; i < input_rows_count; ++i) {
        std::string result_str = result_col->get_data_at(i).to_string();
        ASSERT_EQ(result_str, expected_results[i]);
    }
}

} // namespace doris::vectorized
