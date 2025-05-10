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

#include "util/slice.h"

#include "util/faststring.h"

namespace doris {

// NOTE(zc): we define this function here to make compile work.
Slice::Slice(const faststring& s)
        : // NOLINT(runtime/explicit)
          data((char*)(s.data())),
          size(s.size()) {}

bool Slice::lhs_is_strictly_less_than_rhs(Slice X, bool X_is_truncated, Slice Y,
                                          [[maybe_unused]] bool Y_is_truncated) {
    // suppose X is a prefix of X', Y is a prefix of Y'
    if (!X_is_truncated) {
        // (X_is_truncated == false) means X' == X
        // we have Y <= Y',
        // so X < Y => X < Y',
        // so X' = X < Y'
        return X.compare(Y) < 0;
    }

    // let m = min(|X|,|Y|),
    // we have Y[1..m] = Y'[1..m] <= Y'
    // so X'[1..m] < Y[1..m] => X' < Y'
    std::size_t m {std::min(X.get_size(), Y.get_size())};
    Slice Y_to_cmp {Y.get_data(), m};
    return X.compare(Y_to_cmp) < 0;
}

} // namespace doris
