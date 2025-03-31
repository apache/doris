// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#ifdef _WINDOWS

#ifdef _WINDLL
#define DISKANN_DLLEXPORT __declspec(dllexport)
#else
#define DISKANN_DLLEXPORT __declspec(dllimport)
#endif

#else
#define DISKANN_DLLEXPORT
#endif
