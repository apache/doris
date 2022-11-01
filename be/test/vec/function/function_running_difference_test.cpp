#include <gtest/gtest.h>
#include <time.h>

#include <any>
#include <cmath>
#include <iostream>
#include <string>

#include "function_test_util.h"
namespace doris::vectorized {
    using namespace ut_type;
    TEST(function_running_test, function_running_difference_test) {
    std::string func_name = "running_difference"; 

    InputTypeSet input_types = {TypeIndex::Int64};

    DataSet data_set = {{{(int64_t)0}, (int64_t)0},
                        {{(int64_t)1}, (int64_t)1},
                        {{(int64_t)2}, (int64_t)1},
                        {{(int64_t)3}, (int64_t)1},
                        {{(int64_t)5}, (int64_t)2}};

    check_function<DataTypeInt64, true>(func_name, input_types, data_set);
}

}