#include "string_tmp.h"

namespace doris {

inline StringValue StringValue::trim() const {
    // Remove leading and trailing spaces.
    int32_t begin = 0;

    while (begin < len && ptr[begin] == ' ') {
        ++begin;
    }

    int32_t end = len - 1;

    while (end > begin && ptr[end] == ' ') {
        --end;
    }

    return StringValue(ptr + begin, end - begin + 1);
}
} // namespace doris
