#include "snii/common/status.h"

#include <array>
#include <cstddef>

namespace snii {
namespace {

// Name table in the same order as the StatusCode enum, to avoid a long switch chain in to_string.
constexpr std::array<const char*, 7> kCodeNames = {
        "OK", "Corruption", "NotFound", "InvalidArgument", "IoError", "Unsupported", "Internal"};

} // namespace

std::string Status::to_string() const {
    std::string out = kCodeNames[static_cast<std::size_t>(code_)];
    if (!message_.empty()) {
        out += ": ";
        out += message_;
    }
    return out;
}

} // namespace snii
