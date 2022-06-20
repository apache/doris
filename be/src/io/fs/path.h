#pragma once

#include <filesystem>

namespace doris {
namespace io {

using Path = std::filesystem::path;

inline Path operator/(Path&& lhs, const Path& rhs) {
    return std::move(lhs /= rhs);
}

} // namespace io
} // namespace doris
