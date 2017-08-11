// Copyright 2012 Google Inc. All Rights Reserved.
//
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
//
// Author: tkaftal@google.com (Tomasz Kaftal)
//
// The implementation of walltime functionalities.
#ifndef _GNU_SOURCE   // gcc3 at least defines it on the command line
#define _GNU_SOURCE   // Linux wants that for strptime in time.h
#endif

#include "gutil/walltime.h"

#include <stdio.h>
#include <string.h>

#if defined(__APPLE__)
#include <mach/clock.h>
#include <mach/mach.h>
#endif  // defined(__APPLE__)

#if defined(__APPLE__)
namespace walltime_internal {

GoogleOnceType timebase_info_once = GOOGLE_ONCE_INIT;
mach_timebase_info_data_t timebase_info;

void InitializeTimebaseInfo() {
  CHECK_EQ(KERN_SUCCESS, mach_timebase_info(&timebase_info))
      << "unable to initialize mach_timebase_info";
}
} // namespace walltime_internal
#endif

// This is exactly like mktime() except it is guaranteed to return -1 on
// failure.  Some versions of glibc allow mktime() to return negative
// values which the standard says are undefined.  See the standard at
// http://www.opengroup.org/onlinepubs/007904875/basedefs/xbd_chap04.html
// under the heading "Seconds Since the Epoch".
static inline time_t gmktime(struct tm *tm) {
  time_t rt = mktime(tm);
  return rt < 0 ? time_t(-1) : rt;
}

static void StringAppendStrftime(string* dst,
                                 const char* format,
                                 const struct tm* tm) {
  char space[1024];

  int result = strftime(space, sizeof(space), format, tm);

  if ((result >= 0) && (result < sizeof(space))) {
    // It fit
    dst->append(space, result);
    return;
  }

  int length = sizeof(space);
  for (int sanity = 0; sanity < 5; ++sanity) {
    length *= 2;
    auto buf = new char[length];

    result = strftime(buf, length, format, tm);
    if ((result >= 0) && (result < length)) {
      // It fit
      dst->append(buf, result);
      delete[] buf;
      return;
    }

    delete[] buf;
  }

  // sanity failure
  return;
}

// Convert a "struct tm" interpreted as *GMT* into a time_t (technically
// a long since we can't include header files in header files bla bla bla).
// This is basically filling a hole in the standard library.
//
// There are several approaches to mkgmtime() implementation on the net,
// many of them wrong.  Simply reimplementing the logic seems to be the
// simplest and most efficient, though it does reimplement calendar logic.
// The calculation is mostly straightforward; leap years are the main issue.
//
// Like gmktime() this method returns -1 on failure. Negative results
// are considered undefined by the standard so these cases are
// considered failures and thus return -1.
time_t mkgmtime(const struct tm *tm) {
  // Month-to-day offset for non-leap-years.
  static const int month_day[12] =
    {0, 31, 59, 90, 120, 151, 181, 212, 243, 273, 304, 334};

  // Most of the calculation is easy; leap years are the main difficulty.
  int month = tm->tm_mon % 12;
  int year = tm->tm_year + tm->tm_mon / 12;
  if (month < 0) {   // Negative values % 12 are still negative.
    month += 12;
    --year;
  }

  // This is the number of Februaries since 1900.
  const int year_for_leap = (month > 1) ? year + 1 : year;

  time_t rt = tm->tm_sec                           // Seconds
       + 60 * (tm->tm_min                          // Minute = 60 seconds
       + 60 * (tm->tm_hour                         // Hour = 60 minutes
       + 24 * (month_day[month] + tm->tm_mday - 1  // Day = 24 hours
       + 365 * (year - 70)                         // Year = 365 days
       + (year_for_leap - 69) / 4                  // Every 4 years is leap...
       - (year_for_leap - 1) / 100                 // Except centuries...
       + (year_for_leap + 299) / 400)));           // Except 400s.
  return rt < 0 ? -1 : rt;
}

bool WallTime_Parse_Timezone(const char* time_spec,
                             const char* format,
                             const struct tm* default_time,
                             bool local,
                             WallTime* result) {
  struct tm split_time;
  if (default_time) {
     split_time = *default_time;
  } else {
     memset(&split_time, 0, sizeof(split_time));
  }
  const char* parsed = strptime(time_spec, format, &split_time);
  if (parsed == nullptr) return false;

  // If format ends with "%S", match fractional seconds
  double fraction = 0.0;
  char junk;
  if ((*parsed == '.') &&
     (strcmp(format + strlen(format) - 2, "%S") == 0) &&
     (sscanf(parsed, "%lf%c",  // NOLINT(runtime/printf)
             &fraction, &junk) == 1)) {
     parsed = format + strlen(format);   // Parsed it all!
  }
  if (*parsed != '\0') return false;

  // Convert into seconds since epoch.  Adjust so it is interpreted
  // w.r.t. the daylight-saving-state at the specified time.
  split_time.tm_isdst = -1;     // Ask gmktime() to find dst imfo
  time_t ptime;
  if (local) {
    ptime = gmktime(&split_time);
  } else {
    ptime = mkgmtime(&split_time);  // Returns time in GMT instead of local.
  }

  if (ptime == -1) return false;

  *result = ptime;
  *result += fraction;
  return true;
}

WallTime WallTime_Now() {
#if defined(__APPLE__)
  mach_timespec_t ts;
  walltime_internal::GetCurrentTime(&ts);
  return ts.tv_sec + ts.tv_nsec / static_cast<double>(1e9);
#else
  timespec ts;
  clock_gettime(CLOCK_REALTIME, &ts);
  return ts.tv_sec + ts.tv_nsec / static_cast<double>(1e9);
#endif  // defined(__APPLE__)
}


void StringAppendStrftime(string* dst,
                          const char* format,
                          time_t when,
                          bool local) {
  struct tm tm;
  bool conversion_error;
  if (local) {
    conversion_error = (localtime_r(&when, &tm) == nullptr);
  } else {
    conversion_error = (gmtime_r(&when, &tm) == nullptr);
  }
  if (conversion_error) {
    // If we couldn't convert the time, don't append anything.
    return;
  }
  StringAppendStrftime(dst, format, &tm);
}

string LocalTimeAsString() {
  string ret;
  StringAppendStrftime(&ret, "%Y-%m-%d %H:%M:%S %Z", time(nullptr), true);
  return ret;
}
