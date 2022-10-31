#include "vec/functions/function_running_difference.h"

namespace doris::vectorized {

void register_function_running_difference(SimpleFunctionFactory& factory) {
    factory.register_function<FunctionRunningDifference>();
}

} 