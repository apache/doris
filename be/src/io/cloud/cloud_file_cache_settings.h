#pragma once

#include "io/cloud/cloud_file_cache_fwd.h"

namespace doris {
namespace io {

struct FileCacheSettings {
    size_t max_size = 0;
    size_t max_elements = REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS;
    // use a priority policy to eliminate
    size_t persistent_max_size = 0;
    size_t persistent_max_elements = REMOTE_FS_OBJECTS_CACHE_DEFAULT_ELEMENTS;

    size_t max_file_segment_size = 0;
    size_t max_query_cache_size = 0;
};

} // namespace io
} // namespace doris
