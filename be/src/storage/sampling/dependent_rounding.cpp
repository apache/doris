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

#include "storage/sampling/dependent_rounding.h"

#include <cmath>

#include "common/check.h"

namespace doris {

DependentRounding::DependentRounding(double probability, int64_t seed, int64_t tablet_id)
        : _u(sampling_mix(sampling_mix(sampling_domain("dependent-rounding"), seed), tablet_id)) {
    // A finite binary64 probability in (0, 1] gives 52 <= _shift <= 1126.
    DORIS_CHECK(std::isfinite(probability) && probability > 0.0 && probability <= 1.0);
    int exponent;
    _mantissa = static_cast<uint64_t>(std::ldexp(std::frexp(probability, &exponent), 53));
    _shift = 53 - exponent;
}

uint64_t DependentRounding::cumulative(uint64_t population) const {
    // Compute floor(p * population + u / 2^64). Adding u to a large floating-point
    // prefix can lose even whole units. A binary64 mantissa times a 64-bit count fits in 117 bits.
    using Wide = unsigned __int128;
    const Wide product = static_cast<Wide>(_mantissa) * population;
    const uint64_t whole = _shift >= 128 ? 0 : static_cast<uint64_t>(product >> _shift);
    // Top 64 fractional bits suffice to decide the carry from the 64-bit uniform offset.
    uint64_t fraction = 0;
    if (_shift <= 64) {
        fraction = static_cast<uint64_t>(product << (64 - _shift));
    } else if (_shift < 192) {
        fraction = static_cast<uint64_t>(product >> (_shift - 64));
    }
    return whole + (fraction > ~_u);
}

} // namespace doris
