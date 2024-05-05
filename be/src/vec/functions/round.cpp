// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "round.h"

#include "vec/functions/simple_function_factory.h"

namespace doris::vectorized {

// We split round funcs from register_function_math() in math.cpp to here,
// so that to speed up compile time and make code more readable.
void register_function_round(SimpleFunctionFactory& factory) {
#define REGISTER_ROUND_FUNCTIONS(IMPL)                                                           \
    factory.register_function<                                                                   \
            FunctionRounding<IMPL<TruncateName>, RoundingMode::Trunc, TieBreakingMode::Auto>>(); \
    factory.register_function<                                                                   \
            FunctionRounding<IMPL<FloorName>, RoundingMode::Floor, TieBreakingMode::Auto>>();    \
    factory.register_function<                                                                   \
            FunctionRounding<IMPL<RoundName>, RoundingMode::Round, TieBreakingMode::Auto>>();    \
    factory.register_function<                                                                   \
            FunctionRounding<IMPL<CeilName>, RoundingMode::Ceil, TieBreakingMode::Auto>>();      \
    factory.register_function<FunctionRounding<IMPL<RoundBankersName>, RoundingMode::Round,      \
                                               TieBreakingMode::Bankers>>();

    REGISTER_ROUND_FUNCTIONS(DecimalRoundOneImpl)
    REGISTER_ROUND_FUNCTIONS(DecimalRoundTwoImpl)
    REGISTER_ROUND_FUNCTIONS(DoubleRoundOneImpl)
    REGISTER_ROUND_FUNCTIONS(DoubleRoundTwoImpl)

    factory.register_alias("ceil", "dceil");
    factory.register_alias("ceil", "ceiling");
    factory.register_alias("floor", "dfloor");
    factory.register_alias("round", "dround");
}

} // namespace doris::vectorized
