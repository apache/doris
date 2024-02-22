#include "int8_to_string.h"

#include "vec/core/types.h"

namespace std {
std::string to_string(doris::vectorized::Int8 v) /// NOLINT (cert-dcl58-cpp)
{
    return to_string(int8_t {v});
}
} // namespace std