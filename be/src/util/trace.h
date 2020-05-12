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

#include <iosfwd>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/strings/stringpiece.h"
#include "kudu/gutil/strings/substitute.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/threading/thread_collision_warner.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/locks.h"
#include "kudu/util/trace_metrics.h"

namespace kudu {
class Trace;
}

// Adopt a Trace on the current thread for the duration of the current
// scope. The old current Trace is restored when the scope is exited.
//
// 't' should be a Trace* pointer.
#define ADOPT_TRACE(t) kudu::ScopedAdoptTrace _adopt_trace(t);

// Issue a trace message, if tracing is enabled in the current thread.
// See Trace::SubstituteAndTrace for arguments.
// Example:
//  TRACE("Acquired timestamp $0", timestamp);
#define TRACE(format, substitutions...) \
  do { \
    kudu::Trace* _trace = kudu::Trace::CurrentTrace(); \
    if (_trace) { \
      _trace->SubstituteAndTrace(__FILE__, __LINE__, (format),  \
        ##substitutions); \
    } \
  } while (0)

// Like the above, but takes the trace pointer as an explicit argument.
#define TRACE_TO(trace, format, substitutions...) \
  (trace)->SubstituteAndTrace(__FILE__, __LINE__, (format), ##substitutions)

// Increment a counter associated with the current trace.
//
// Each trace contains a map of counters which can be used to keep
// request-specific statistics. It is significantly faster to increment
// a trace counter compared to logging a message. Additionally, having
// slightly more structured information makes it easier to aggregate
// and show information back to operators.
//
// NOTE: the 'counter_name' MUST be a string which stays alive forever.
// Typically, this is a compile-time constant. If something other than
// a constant is required, use TraceMetric::InternName() in order to
// create a string which will last for the process lifetime. Of course,
// these strings will never be cleaned up, so it's important to use this
// judiciously.
//
// If no trace is active, this does nothing and does not evaluate its
// parameters.
#define TRACE_COUNTER_INCREMENT(counter_name, val) \
  do { \
    kudu::Trace* _trace = kudu::Trace::CurrentTrace(); \
    if (_trace) { \
      _trace->metrics()->Increment(counter_name, val); \
    } \
  } while (0)

// Increment a counter for the amount of wall time spent in the current
// scope. For example:
//
//  void DoFoo() {
//    TRACE_COUNTER_SCOPE_LATENCY_US("foo_us");
//    ... do expensive Foo thing
//  }
//
//  will result in a trace metric indicating the number of microseconds spent
//  in invocations of DoFoo().
#define TRACE_COUNTER_SCOPE_LATENCY_US(counter_name) \
  ::kudu::ScopedTraceLatencyCounter _scoped_latency(counter_name)

// Construct a constant C string counter name which acts as a sort of
// coarse-grained histogram for trace metrics.
#define BUCKETED_COUNTER_NAME(prefix, duration_us)      \
  [=]() {                                               \
    if (duration_us >= 100 * 1000) {                    \
      return prefix "_gt_100_ms";                       \
    } else if (duration_us >= 10 * 1000) {              \
      return prefix "_10-100_ms";                       \
    } else if (duration_us >= 1000) {                   \
      return prefix "_1-10_ms";                         \
    } else {                                            \
      return prefix "_lt_1ms";                          \
    }                                                   \
  }()

namespace kudu {

class JsonWriter;
class ThreadSafeArena;
struct TraceEntry;

// A trace for a request or other process. This supports collecting trace entries
// from a number of threads, and later dumping the results to a stream.
//
// Callers should generally not add trace messages directly using the public
// methods of this class. Rather, the TRACE(...) macros defined above should
// be used such that file/line numbers are automatically included, etc.
//
// This class is thread-safe.
class Trace : public RefCountedThreadSafe<Trace> {
 public:
  Trace();

  // Logs a message into the trace buffer.
  //
  // See strings::Substitute for details.
  //
  // N.B.: the file path passed here is not copied, so should be a static
  // constant (eg __FILE__).
  void SubstituteAndTrace(const char* filepath, int line_number,
                          StringPiece format,
                          const strings::internal::SubstituteArg& arg0 =
                            strings::internal::SubstituteArg::kNoArg,
                          const strings::internal::SubstituteArg& arg1 =
                            strings::internal::SubstituteArg::kNoArg,
                          const strings::internal::SubstituteArg& arg2 =
                            strings::internal::SubstituteArg::kNoArg,
                          const strings::internal::SubstituteArg& arg3 =
                            strings::internal::SubstituteArg::kNoArg,
                          const strings::internal::SubstituteArg& arg4 =
                            strings::internal::SubstituteArg::kNoArg,
                          const strings::internal::SubstituteArg& arg5 =
                            strings::internal::SubstituteArg::kNoArg,
                          const strings::internal::SubstituteArg& arg6 =
                            strings::internal::SubstituteArg::kNoArg,
                          const strings::internal::SubstituteArg& arg7 =
                            strings::internal::SubstituteArg::kNoArg,
                          const strings::internal::SubstituteArg& arg8 =
                            strings::internal::SubstituteArg::kNoArg,
                          const strings::internal::SubstituteArg& arg9 =
                            strings::internal::SubstituteArg::kNoArg);

  // Dump the trace buffer to the given output stream.
  //
  enum {
    NO_FLAGS = 0,

    // If set, calculate and print the difference between successive trace messages.
    INCLUDE_TIME_DELTAS = 1 << 0,
    // If set, include a 'Metrics' line showing any attached trace metrics.
    INCLUDE_METRICS =     1 << 1,

    INCLUDE_ALL = INCLUDE_TIME_DELTAS | INCLUDE_METRICS
  };
  void Dump(std::ostream* out, int flags) const;

  // Dump the trace buffer as a string.
  std::string DumpToString(int flags = INCLUDE_ALL) const;

  std::string MetricsAsJSON() const;

  // Attaches the given trace which will get appended at the end when Dumping.
  //
  // The 'label' does not necessarily have to be unique, and is used to identify
  // the child trace when dumped. The contents of the StringPiece are copied
  // into this trace's arena.
  void AddChildTrace(StringPiece label, Trace* child_trace);

  // Return a copy of the current set of related "child" traces.
  std::vector<std::pair<StringPiece, scoped_refptr<Trace>>> ChildTraces() const;

  // Return the current trace attached to this thread, if there is one.
  static Trace* CurrentTrace() {
    return threadlocal_trace_;
  }

  // Simple function to dump the current trace to stderr, if one is
  // available. This is meant for usage when debugging in gdb via
  // 'call kudu::Trace::DumpCurrentTrace();'.
  static void DumpCurrentTrace();

  TraceMetrics* metrics() {
    return &metrics_;
  }
  const TraceMetrics& metrics() const {
    return metrics_;
  }

 private:
  friend class ScopedAdoptTrace;
  friend class RefCountedThreadSafe<Trace>;
  ~Trace();

  // The current trace for this thread. Threads should only set this using
  // using ScopedAdoptTrace, which handles reference counting the underlying
  // object.
  static __thread Trace* threadlocal_trace_;

  // Allocate a new entry from the arena, with enough space to hold a
  // message of length 'len'.
  TraceEntry* NewEntry(int len, const char* file_path, int line_number);

  // Add the entry to the linked list of entries.
  void AddEntry(TraceEntry* entry);

  void MetricsToJSON(JsonWriter* jw) const;

  std::unique_ptr<ThreadSafeArena> arena_;

  // Lock protecting the entries linked list.
  mutable simple_spinlock lock_;
  // The head of the linked list of entries (allocated inside arena_)
  TraceEntry* entries_head_;
  // The tail of the linked list of entries (allocated inside arena_)
  TraceEntry* entries_tail_;

  std::vector<std::pair<StringPiece, scoped_refptr<Trace>>> child_traces_;

  TraceMetrics metrics_;

  DISALLOW_COPY_AND_ASSIGN(Trace);
};

// Adopt a Trace object into the current thread for the duration
// of this object.
// This should only be used on the stack (and thus created and destroyed
// on the same thread)
class ScopedAdoptTrace {
 public:
  explicit ScopedAdoptTrace(Trace* t) :
    old_trace_(Trace::threadlocal_trace_) {
    Trace::threadlocal_trace_ = t;
    if (t) {
      t->AddRef();
    }
    DFAKE_SCOPED_LOCK_THREAD_LOCKED(ctor_dtor_);
  }

  ~ScopedAdoptTrace() {
    if (Trace::threadlocal_trace_) {
      Trace::threadlocal_trace_->Release();
    }
    Trace::threadlocal_trace_ = old_trace_;
    DFAKE_SCOPED_LOCK_THREAD_LOCKED(ctor_dtor_);
  }

 private:
  DFAKE_MUTEX(ctor_dtor_);
  Trace* old_trace_;

  DISALLOW_COPY_AND_ASSIGN(ScopedAdoptTrace);
};

// Implementation for TRACE_COUNTER_SCOPE_LATENCY_US(...) macro above.
class ScopedTraceLatencyCounter {
 public:
  explicit ScopedTraceLatencyCounter(const char* counter)
      : counter_(counter),
        start_time_(GetCurrentTimeMicros()) {
  }

  ~ScopedTraceLatencyCounter() {
    TRACE_COUNTER_INCREMENT(counter_, GetCurrentTimeMicros() - start_time_);
  }

 private:
  const char* const counter_;
  MicrosecondsInt64 start_time_;
  DISALLOW_COPY_AND_ASSIGN(ScopedTraceLatencyCounter);
};

} // namespace kudu
