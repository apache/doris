// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.
#pragma once

#include <functional>
#include <iostream>
#include "windows_customizations.h"

#ifdef EXEC_ENV_OLS
#ifndef ENABLE_CUSTOM_LOGGER
#define ENABLE_CUSTOM_LOGGER
#endif // !ENABLE_CUSTOM_LOGGER
#endif // EXEC_ENV_OLS

namespace diskann
{
#ifdef ENABLE_CUSTOM_LOGGER
DISKANN_DLLEXPORT extern std::basic_ostream<char> cout;
DISKANN_DLLEXPORT extern std::basic_ostream<char> cerr;
#else
using std::cerr;
using std::cout;
#endif

enum class DISKANN_DLLEXPORT LogLevel
{
    LL_Info = 0,
    LL_Error,
    LL_Count
};

#ifdef ENABLE_CUSTOM_LOGGER
DISKANN_DLLEXPORT void SetCustomLogger(std::function<void(LogLevel, const char *)> logger);
#endif
} // namespace diskann
