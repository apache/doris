#pragma once
#include <memory>

#include "vec/common/uint128.h"

static constexpr size_t GB = 1 * 1024 * 1024 * 1024;
static constexpr size_t KB = 1024;
namespace doris {
namespace io {

using uint128_t = vectorized::UInt128;
using UInt128Hash = vectorized::UInt128Hash;
static constexpr size_t REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS = 100 * 1024;

struct FileCacheSettings;
} // namespace io
} // namespace doris
