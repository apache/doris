// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <cpprest/base_uri.h>
#include <restapi/search_wrapper.h>

namespace diskann
{
// Constants
static const std::string VECTOR_KEY = "query", K_KEY = "k", INDICES_KEY = "indices", DISTANCES_KEY = "distances",
                         TAGS_KEY = "tags", QUERY_ID_KEY = "query_id", ERROR_MESSAGE_KEY = "error", L_KEY = "Ls",
                         TIME_TAKEN_KEY = "time_taken_in_us", PARTITION_KEY = "partition",
                         UNKNOWN_ERROR = "unknown_error";
const unsigned int DEFAULT_L = 100;

} // namespace diskann