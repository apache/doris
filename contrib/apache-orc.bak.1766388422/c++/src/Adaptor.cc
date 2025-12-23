/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Adaptor.hh"
#include <iomanip>
#include <sstream>

#ifndef HAS_STRPTIME
char* strptime(const char* s, const char* f, struct tm* tm) {
  std::istringstream input(s);
  input.imbue(std::locale(setlocale(LC_ALL, nullptr)));
  input >> std::get_time(tm, f);
  if (input.fail()) return nullptr;
  return (char*)(s + input.tellg());
}
#endif

#ifndef HAS_PREAD
#ifdef _WIN32
#include <Windows.h>
#include <io.h>
ssize_t pread(int fd, void* buf, size_t size, off_t offset) {
  auto handle = reinterpret_cast<HANDLE>(_get_osfhandle(fd));

  OVERLAPPED ol;
  memset(&ol, 0, sizeof(OVERLAPPED));
  ol.Offset = offset;

  DWORD rt;
  if (!ReadFile(handle, buf, static_cast<DWORD>(size), &rt, &ol)) {
    errno = GetLastError();
    return -1;
  }
  return static_cast<ssize_t>(rt);
}
#else
#error("pread() undefined: unknown environment")
#endif
#endif

namespace orc {
#ifdef HAS_DOUBLE_TO_STRING
  std::string to_string(double val) {
    return std::to_string(val);
  }
#else
  std::string to_string(double val) {
    return std::to_string(static_cast<long double>(val));
  }
#endif

#ifdef HAS_INT64_TO_STRING
  std::string to_string(int64_t val) {
    return std::to_string(val);
  }
#else
  std::string to_string(int64_t val) {
    return std::to_string(static_cast<long long int>(val));
  }
#endif
}  // namespace orc
