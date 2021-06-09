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

#include "util/monotime.h"

#include <sys/time.h>

#include <limits>

#include "common/logging.h"
#include "gutil/sysinfo.h"

namespace doris {

#define MAX_MONOTONIC_SECONDS (((1ULL << 63) - 1ULL) / (int64_t)MonoTime::kNanosecondsPerSecond)

const int64_t MonoDelta::kUninitialized = kint64min;

MonoDelta MonoDelta::FromSeconds(double seconds) {
    int64_t delta = seconds * MonoTime::kNanosecondsPerSecond;
    return MonoDelta(delta);
}

MonoDelta MonoDelta::FromMilliseconds(int64_t ms) {
    return MonoDelta(ms * MonoTime::kNanosecondsPerMillisecond);
}

MonoDelta MonoDelta::FromMicroseconds(int64_t us) {
    return MonoDelta(us * MonoTime::kNanosecondsPerMicrosecond);
}

MonoDelta MonoDelta::FromNanoseconds(int64_t ns) {
    return MonoDelta(ns);
}

MonoDelta::MonoDelta() : nano_delta_(kUninitialized) {}

bool MonoDelta::Initialized() const {
    return nano_delta_ != kUninitialized;
}

bool MonoDelta::LessThan(const MonoDelta& rhs) const {
    DCHECK(Initialized());
    DCHECK(rhs.Initialized());
    return nano_delta_ < rhs.nano_delta_;
}

bool MonoDelta::MoreThan(const MonoDelta& rhs) const {
    DCHECK(Initialized());
    DCHECK(rhs.Initialized());
    return nano_delta_ > rhs.nano_delta_;
}

bool MonoDelta::Equals(const MonoDelta& rhs) const {
    DCHECK(Initialized());
    DCHECK(rhs.Initialized());
    return nano_delta_ == rhs.nano_delta_;
}

std::string MonoDelta::ToString() const {
    char buf[1024] = {'\0'};
    snprintf(buf, sizeof(buf), "%.3f", ToSeconds());
    return std::string(buf);
}

MonoDelta::MonoDelta(int64_t delta) : nano_delta_(delta) {}

double MonoDelta::ToSeconds() const {
    DCHECK(Initialized());
    double d(nano_delta_);
    d /= MonoTime::kNanosecondsPerSecond;
    return d;
}

int64_t MonoDelta::ToNanoseconds() const {
    DCHECK(Initialized());
    return nano_delta_;
}

int64_t MonoDelta::ToMicroseconds() const {
    DCHECK(Initialized());
    return nano_delta_ / MonoTime::kNanosecondsPerMicrosecond;
}

int64_t MonoDelta::ToMilliseconds() const {
    DCHECK(Initialized());
    return nano_delta_ / MonoTime::kNanosecondsPerMillisecond;
}

void MonoDelta::ToTimeVal(struct timeval* tv) const {
    DCHECK(Initialized());
    tv->tv_sec = nano_delta_ / MonoTime::kNanosecondsPerSecond;
    tv->tv_usec = (nano_delta_ - (tv->tv_sec * MonoTime::kNanosecondsPerSecond)) /
                  MonoTime::kNanosecondsPerMicrosecond;

    // tv_usec must be between 0 and 999999.
    // There is little use for negative timevals so wrap it in PREDICT_FALSE.
    if (PREDICT_FALSE(tv->tv_usec < 0)) {
        --(tv->tv_sec);
        tv->tv_usec += 1000000;
    }

    // Catch positive corner case where we "round down" and could potentially set a timeout of 0.
    // Make it 1 usec.
    if (PREDICT_FALSE(tv->tv_usec == 0 && tv->tv_sec == 0 && nano_delta_ > 0)) {
        tv->tv_usec = 1;
    }

    // Catch negative corner case where we "round down" and could potentially set a timeout of 0.
    // Make it -1 usec (but normalized, so tv_usec is not negative).
    if (PREDICT_FALSE(tv->tv_usec == 0 && tv->tv_sec == 0 && nano_delta_ < 0)) {
        tv->tv_sec = -1;
        tv->tv_usec = 999999;
    }
}

void MonoDelta::NanosToTimeSpec(int64_t nanos, struct timespec* ts) {
    ts->tv_sec = nanos / MonoTime::kNanosecondsPerSecond;
    ts->tv_nsec = nanos - (ts->tv_sec * MonoTime::kNanosecondsPerSecond);

    // tv_nsec must be between 0 and 999999999.
    // There is little use for negative timespecs so wrap it in PREDICT_FALSE.
    if (PREDICT_FALSE(ts->tv_nsec < 0)) {
        --(ts->tv_sec);
        ts->tv_nsec += MonoTime::kNanosecondsPerSecond;
    }
}

void MonoDelta::ToTimeSpec(struct timespec* ts) const {
    DCHECK(Initialized());
    NanosToTimeSpec(nano_delta_, ts);
}

MonoTime MonoTime::Now() {
    struct timespec ts;
    PCHECK(clock_gettime(CLOCK_MONOTONIC, &ts) == 0);
    return MonoTime(ts);
}

MonoTime MonoTime::Max() {
    return MonoTime(std::numeric_limits<int64_t>::max());
}

MonoTime MonoTime::Min() {
    return MonoTime(1);
}

const MonoTime& MonoTime::Earliest(const MonoTime& a, const MonoTime& b) {
    if (b.nanos_ < a.nanos_) {
        return b;
    }
    return a;
}

MonoTime::MonoTime() : nanos_(0) {}

bool MonoTime::Initialized() const {
    return nanos_ != 0;
}

MonoDelta MonoTime::GetDeltaSince(const MonoTime& rhs) const {
    DCHECK(Initialized());
    DCHECK(rhs.Initialized());
    int64_t delta(nanos_);
    delta -= rhs.nanos_;
    return MonoDelta(delta);
}

void MonoTime::AddDelta(const MonoDelta& delta) {
    DCHECK(Initialized());
    nanos_ += delta.nano_delta_;
}

bool MonoTime::ComesBefore(const MonoTime& rhs) const {
    DCHECK(Initialized());
    DCHECK(rhs.Initialized());
    return nanos_ < rhs.nanos_;
}

std::string MonoTime::ToString() const {
    char buf[1024] = {'\0'};
    snprintf(buf, sizeof(buf), "%.3f", ToSeconds());
    return std::string(buf);
}

void MonoTime::ToTimeSpec(struct timespec* ts) const {
    DCHECK(Initialized());
    MonoDelta::NanosToTimeSpec(nanos_, ts);
}

bool MonoTime::Equals(const MonoTime& other) const {
    return nanos_ == other.nanos_;
}

MonoTime& MonoTime::operator+=(const MonoDelta& delta) {
    this->AddDelta(delta);
    return *this;
}

MonoTime& MonoTime::operator-=(const MonoDelta& delta) {
    this->AddDelta(MonoDelta(-1 * delta.nano_delta_));
    return *this;
}

MonoTime::MonoTime(const struct timespec& ts) {
    // Monotonic time resets when the machine reboots.  The 64-bit limitation
    // means that we can't represent times larger than 292 years, which should be
    // adequate.
    CHECK_LT(ts.tv_sec, MAX_MONOTONIC_SECONDS);
    nanos_ = ts.tv_sec;
    nanos_ *= MonoTime::kNanosecondsPerSecond;
    nanos_ += ts.tv_nsec;
}

MonoTime::MonoTime(int64_t nanos) : nanos_(nanos) {}

double MonoTime::ToSeconds() const {
    double d(nanos_);
    d /= MonoTime::kNanosecondsPerSecond;
    return d;
}

void SleepFor(const MonoDelta& delta) {
    base::SleepForNanoseconds(delta.ToNanoseconds());
}

bool operator==(const MonoDelta& lhs, const MonoDelta& rhs) {
    return lhs.Equals(rhs);
}

bool operator!=(const MonoDelta& lhs, const MonoDelta& rhs) {
    return !lhs.Equals(rhs);
}

bool operator<(const MonoDelta& lhs, const MonoDelta& rhs) {
    return lhs.LessThan(rhs);
}

bool operator<=(const MonoDelta& lhs, const MonoDelta& rhs) {
    return lhs.LessThan(rhs) || lhs.Equals(rhs);
}

bool operator>(const MonoDelta& lhs, const MonoDelta& rhs) {
    return lhs.MoreThan(rhs);
}

bool operator>=(const MonoDelta& lhs, const MonoDelta& rhs) {
    return lhs.MoreThan(rhs) || lhs.Equals(rhs);
}

bool operator==(const MonoTime& lhs, const MonoTime& rhs) {
    return lhs.Equals(rhs);
}

bool operator!=(const MonoTime& lhs, const MonoTime& rhs) {
    return !lhs.Equals(rhs);
}

bool operator<(const MonoTime& lhs, const MonoTime& rhs) {
    return lhs.ComesBefore(rhs);
}

bool operator<=(const MonoTime& lhs, const MonoTime& rhs) {
    return lhs.ComesBefore(rhs) || lhs.Equals(rhs);
}

bool operator>(const MonoTime& lhs, const MonoTime& rhs) {
    return rhs.ComesBefore(lhs);
}

bool operator>=(const MonoTime& lhs, const MonoTime& rhs) {
    return rhs.ComesBefore(lhs) || rhs.Equals(lhs);
}

MonoTime operator+(const MonoTime& t, const MonoDelta& delta) {
    MonoTime tmp(t);
    tmp.AddDelta(delta);
    return tmp;
}

MonoTime operator-(const MonoTime& t, const MonoDelta& delta) {
    MonoTime tmp(t);
    tmp.AddDelta(MonoDelta::FromNanoseconds(-delta.ToNanoseconds()));
    return tmp;
}

MonoDelta operator-(const MonoTime& t_end, const MonoTime& t_beg) {
    return t_end.GetDeltaSince(t_beg);
}

} // namespace doris
