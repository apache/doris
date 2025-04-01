// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
#pragma once

#include <mutex>

#ifdef _WINDOWS
#include "windows_slim_lock.h"
#endif

namespace diskann
{
#ifdef _WINDOWS
using non_recursive_mutex = windows_exclusive_slim_lock;
using LockGuard = windows_exclusive_slim_lock_guard;
#else
using non_recursive_mutex = std::mutex;
using LockGuard = std::lock_guard<non_recursive_mutex>;
#endif
} // namespace diskann
