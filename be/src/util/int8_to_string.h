#pragma once

#include <fmt/format.h>

#include "vec/core/types.h"

template <>
struct fmt::formatter<doris::vectorized::Int8> : fmt::formatter<int8_t> {};

namespace std {
std::string to_string(doris::vectorized::Int8 v); /// NOLINT (cert-dcl58-cpp)
}