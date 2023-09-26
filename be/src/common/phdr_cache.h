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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/base/phdr_cache.h
// and modified by Doris

#pragma once

/// This code was based on the code by Fedor Korotkiy https://www.linkedin.com/in/fedor-korotkiy-659a1838/

/** Collects all dl_phdr_info items and caches them in a static array.
  * Also rewrites dl_iterate_phdr with a lock-free version which consults the above cache
  * thus eliminating scalability bottleneck in C++ exception unwinding.
  * As a drawback, this only works if no dynamic object unloading happens after this point.
  * This function is thread-safe. You should call it to update cache after loading new shared libraries.
  * Otherwise exception handling from dlopened libraries won't work (will call std::terminate immediately).
  * NOTE: dlopen is forbidden in our code.
  *
  * NOTE: It is disabled with Thread Sanitizer because TSan can only use original "dl_iterate_phdr" function.
  */
void updatePHDRCache();

/** Check if "dl_iterate_phdr" will be lock-free
  * to determine if some features like Query Profiler can be used.
  */
bool hasPHDRCache();
