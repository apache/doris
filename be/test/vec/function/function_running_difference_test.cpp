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
    {
        InputTypeSet input_types = {TypeIndex::Int64};

        DataSet data_set = {{{(int64_t)0}, (int64_t)0},
                            {{(int64_t)1}, (int64_t)1},
                            {{(int64_t)2}, (int64_t)1},
                            {{(int64_t)3}, (int64_t)1},
                            {{(int64_t)5}, (int64_t)2}};

       check_function<DataTypeInt64, true>(func_name, input_types, data_set);
   }
    {   
        InputTypeSet input_types = {TypeIndex::Float64};
        DataSet data_set = {{{(double)0.0},  (double)0.0}, 
                            {{(double)1.54}, (double)1.54},
                            {{(double)2.33},  (double)0.79},
                            {{(double)8.45}, (double)6.12},
                            {{(double)4.22}, (double)-4.23}};
        check_function<DataTypeFloat64, true>(func_name, input_types, data_set);
    }
    // {
    //     InputTypeSet input_types = {TypeIndex::DateTime};
    //     DataSet data_set = {{{std::string("2019-07-18 12:00:00")},  (int64_t)0}, 
    //                         {{std::string("2019-07-18 12:00:05")}, (int64_t)5},
    //                         {{std::string("2019-07-18 12:00:06")},  (int64_t)1},
    //                         {{std::string("2019-07-18 12:00:08")}, (int64_t)2},
    //                         {{std::string("2019-07-18 12:00:10")}, (int64_t)2}};
    //     check_function<DataTypeInt64, true>(func_name, input_types, data_set); 
    // }
}

}