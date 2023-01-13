#include "string_ref.h"

namespace doris {

inline StringRef StringRef::trim() const {
    // Remove leading and trailing spaces.
    int32_t begin = 0;

    while (begin < size && data[begin] == ' ') {
        ++begin;
    }

    int32_t end = size - 1;

    while (end > begin && data[end] == ' ') {
        --end;
    }

    return StringRef(data + begin, end - begin + 1);
}

// TODO: rewrite in AVX2
inline size_t StringRef::find_first_of(char c) const {
    const char* p = static_cast<const char*>(memchr(data, c, size));
    return p == nullptr ? -1 : p - data;
}

StringRef StringRef::min_string_val() {
    return StringRef((char*)(&StringRef::MIN_CHAR), 1);
}

StringRef StringRef::max_string_val() {
    return StringRef((char*)(&StringRef::MAX_CHAR), 1);
}

bool StringRef::start_with(const StringRef& search_string) const {
    DCHECK(size >= search_string.size);
    if (search_string.size == 0)
        return true;

#if defined(__SSE2__) || defined(__aarch64__)
    return memequalSSE2Wide(data, search_string.data, search_string.size);
#else
    return 0 == memcmp(data, search_string.data, search_string.size);
#endif
}
bool StringRef::end_with(const StringRef& search_string) const {
    DCHECK(size >= search_string.size);
    if (search_string.size == 0)
        return true;

#if defined(__SSE2__) || defined(__aarch64__)
    return memequalSSE2Wide(data + size - search_string.size, search_string.data,
                            search_string.size);
#else
    return 0 == memcmp(data + size - search_string.size, search_string.data, search_string.size);
#endif
}
} // namespace doris
