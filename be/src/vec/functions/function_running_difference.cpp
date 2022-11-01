#include "vec/functions/function_running_difference.h"

namespace doris::vectorized {

void register_function_running_difference(SimpleFunctionFactory& factory) {
    //factory.register_function<FunctionRunningDifferenceImpl<false>>();
    factory.register_function<FunctionRunningDifference>();
    //factory.register_function<FunctionCase<false, true>>();
    //factory.register_function<FunctionCase<true, false>>();
    //factory.register_function<FunctionCase<true, true>>();
}

} // namespace doris::vectorized