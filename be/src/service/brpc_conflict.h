// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#pragma once

// This file is used to fixed macro conflict between butil and gutil
// and this file must put the first include in source file

#include "gutil/macros.h"
// Macros in the guti/macros.h, use butil's define
#ifdef DISALLOW_IMPLICIT_CONSTRUCTORS
#undef DISALLOW_IMPLICIT_CONSTRUCTORS
#endif

#ifdef arraysize
#undef arraysize
#endif

#ifdef ARRAY_SIZE
#undef ARRAY_SIZE
#endif

#undef OVERRIDE
#undef FINAL

// use be/src/gutil/integral_types.h override butil/basictypes.h
#include "gutil/integral_types.h"
#ifdef BASE_INTEGRAL_TYPES_H_
#define BUTIL_BASICTYPES_H_
#endif

#ifdef DEBUG_MODE
#undef DEBUG_MODE
#endif
