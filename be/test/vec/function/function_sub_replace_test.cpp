
#include <gtest/gtest.h>

#include "vec/core/block.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/functions/function_string.h"

namespace doris::vectorized {
TEST(SubReplaceTest, test) {
    const int rows = 10240;
    auto str = ColumnString::create();
    auto new_str = ColumnString::create();
    auto start = ColumnInt32::create();
    auto length = ColumnInt32::create();

    for (int i = 0; i < rows; i++) {
        str->insert_default();
        new_str->insert_default();
        start->insert_default();
        length->insert_default();
    }

    Block block {
            ColumnWithTypeAndName {std::move(str), std::make_shared<DataTypeString>(), "str"},
            ColumnWithTypeAndName {std::move(new_str), std::make_shared<DataTypeString>(),
                                   "new_str"},
            ColumnWithTypeAndName {std::move(start), std::make_shared<DataTypeInt32>(), "start"},
            ColumnWithTypeAndName {std::move(length), std::make_shared<DataTypeInt32>(), "length"},
            ColumnWithTypeAndName {nullptr, std::make_shared<DataTypeInt32>(), "res"},
    };

    EXPECT_TRUE(SubReplaceImpl::replace_execute(block, ColumnNumbers {0, 1, 2, 3}, 4, rows));
}
} // namespace doris::vectorized
