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

#include "kudu/util/trace.h"

#include <cctype>
#include <cstdint>
#include <cstring>
#include <functional>
#include <map>
#include <ostream>
#include <string>
#include <thread>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <rapidjson/document.h>
#include <rapidjson/rapidjson.h>

#include "kudu/gutil/macros.h"
#include "kudu/gutil/port.h"
#include "kudu/gutil/ref_counted.h"
#include "kudu/gutil/walltime.h"
#include "kudu/util/atomic.h"
#include "kudu/util/countdown_latch.h"
#include "kudu/util/debug/trace_event.h"
#include "kudu/util/debug/trace_event_impl.h"
#include "kudu/util/debug/trace_event_synthetic_delay.h"
#include "kudu/util/debug/trace_logging.h"
#include "kudu/util/monotime.h"
#include "kudu/util/scoped_cleanup.h"
#include "kudu/util/status.h"
#include "kudu/util/stopwatch.h"
#include "kudu/util/test_macros.h"
#include "kudu/util/test_util.h"
#include "kudu/util/thread.h"
#include "kudu/util/trace_metrics.h"

using kudu::debug::TraceLog;
using kudu::debug::TraceResultBuffer;
using kudu::debug::CategoryFilter;
using rapidjson::Document;
using rapidjson::Value;
using std::string;
using std::thread;
using std::vector;

namespace kudu {

class TraceTest : public KuduTest {
};

// Replace all digits in 's' with the character 'X'.
static string XOutDigits(const string& s) {
  string ret;
  ret.reserve(s.size());
  for (char c : s) {
    if (isdigit(c)) {
      ret.push_back('X');
    } else {
      ret.push_back(c);
    }
  }
  return ret;
}

TEST_F(TraceTest, TestBasic) {
  scoped_refptr<Trace> t(new Trace);
  TRACE_TO(t, "hello $0, $1", "world", 12345);
  TRACE_TO(t, "goodbye $0, $1", "cruel world", 54321);

  string result = XOutDigits(t->DumpToString(Trace::NO_FLAGS));
  ASSERT_EQ("XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] hello world, XXXXX\n"
            "XXXX XX:XX:XX.XXXXXX trace-test.cc:XX] goodbye cruel world, XXXXX\n",
            result);
}

TEST_F(TraceTest, TestAttach) {
  scoped_refptr<Trace> traceA(new Trace);
  scoped_refptr<Trace> traceB(new Trace);
  {
    ADOPT_TRACE(traceA.get());
    EXPECT_EQ(traceA.get(), Trace::CurrentTrace());
    {
      ADOPT_TRACE(traceB.get());
      EXPECT_EQ(traceB.get(), Trace::CurrentTrace());
      TRACE("hello from traceB");
    }
    EXPECT_EQ(traceA.get(), Trace::CurrentTrace());
    TRACE("hello from traceA");
  }
  EXPECT_TRUE(Trace::CurrentTrace() == nullptr);
  TRACE("this goes nowhere");

  EXPECT_EQ("XXXX XX:XX:XX.XXXXXX trace-test.cc:XXX] hello from traceA\n",
            XOutDigits(traceA->DumpToString(Trace::NO_FLAGS)));
  EXPECT_EQ("XXXX XX:XX:XX.XXXXXX trace-test.cc:XXX] hello from traceB\n",
            XOutDigits(traceB->DumpToString(Trace::NO_FLAGS)));
}

TEST_F(TraceTest, TestChildTrace) {
  scoped_refptr<Trace> traceA(new Trace);
  scoped_refptr<Trace> traceB(new Trace);
  ADOPT_TRACE(traceA.get());
  traceA->AddChildTrace("child", traceB.get());
  TRACE("hello from traceA");
  {
    ADOPT_TRACE(traceB.get());
    TRACE("hello from traceB");
  }
  EXPECT_EQ("XXXX XX:XX:XX.XXXXXX trace-test.cc:XXX] hello from traceA\n"
            "Related trace 'child':\n"
            "XXXX XX:XX:XX.XXXXXX trace-test.cc:XXX] hello from traceB\n",
            XOutDigits(traceA->DumpToString(Trace::NO_FLAGS)));
}

static void GenerateTraceEvents(int thread_id,
                                int num_events) {
  for (int i = 0; i < num_events; i++) {
    TRACE_EVENT1("test", "foo", "thread_id", thread_id);
  }
}

// Parse the dumped trace data and return the number of events
// found within, including only those with the "test" category.
int ParseAndReturnEventCount(const string& trace_json) {
  Document d;
  d.Parse<0>(trace_json.c_str());
  CHECK(d.IsObject()) << "bad json: " << trace_json;
  const Value& events_json = d["traceEvents"];
  CHECK(events_json.IsArray()) << "bad json: " << trace_json;

  // Count how many of our events were seen. We have to filter out
  // the metadata events.
  int seen_real_events = 0;
  for (int i = 0; i < events_json.Size(); i++) {
    if (events_json[i]["cat"].GetString() == string("test")) {
      seen_real_events++;
    }
  }

  return seen_real_events;
}

TEST_F(TraceTest, TestChromeTracing) {
  const int kNumThreads = 4;
  const int kEventsPerThread = AllowSlowTests() ? 1000000 : 10000;

  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 TraceLog::RECORD_CONTINUOUSLY);

  vector<scoped_refptr<Thread> > threads(kNumThreads);

  Stopwatch s;
  s.start();
  for (int i = 0; i < kNumThreads; i++) {
    CHECK_OK(Thread::CreateWithFlags(
        "test", "gen-traces",
        [i, kEventsPerThread]() { GenerateTraceEvents(i, kEventsPerThread); },
        Thread::NO_STACK_WATCHDOG, &threads[i]));
  }

  for (int i = 0; i < kNumThreads; i++) {
    threads[i]->Join();
  }
  tl->SetDisabled();

  int total_events = kNumThreads * kEventsPerThread;
  double elapsed = s.elapsed().wall_seconds();

  LOG(INFO) << "Trace performance: " << static_cast<int>(total_events / elapsed) << " traces/sec";

  string trace_json = TraceResultBuffer::FlushTraceLogToString();

  // Verify that the JSON contains events. It won't have exactly
  // kEventsPerThread * kNumThreads because the trace buffer isn't large enough
  // for that.
  ASSERT_GE(ParseAndReturnEventCount(trace_json), 100);
}

// Test that, if a thread exits before filling a full trace buffer, we still
// see its results. This is a regression test for a bug in the earlier integration
// of Chromium tracing into Kudu.
TEST_F(TraceTest, TestTraceFromExitedThread) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 TraceLog::RECORD_CONTINUOUSLY);

  // Generate 10 trace events in a separate thread.
  int kNumEvents = 10;
  scoped_refptr<Thread> t;
  CHECK_OK(Thread::CreateWithFlags(
      "test", "gen-traces", [kNumEvents]() { GenerateTraceEvents(1, kNumEvents); },
      Thread::NO_STACK_WATCHDOG, &t));
  t->Join();
  tl->SetDisabled();
  string trace_json = TraceResultBuffer::FlushTraceLogToString();
  LOG(INFO) << trace_json;

  // Verify that the buffer contains 10 trace events
  ASSERT_EQ(10, ParseAndReturnEventCount(trace_json));
}

static void GenerateWideSpan() {
  TRACE_EVENT0("test", "GenerateWideSpan");
  for (int i = 0; i < 1000; i++) {
    TRACE_EVENT0("test", "InnerLoop");
  }
}

// Test creating a trace event which contains many other trace events.
// This ensures that we can go back and update a TraceEvent which fell in
// a different trace chunk.
TEST_F(TraceTest, TestWideSpan) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 TraceLog::RECORD_CONTINUOUSLY);

  scoped_refptr<Thread> t;
  CHECK_OK(Thread::CreateWithFlags(
      "test", "gen-traces", &GenerateWideSpan,
      Thread::NO_STACK_WATCHDOG, &t));
  t->Join();
  tl->SetDisabled();

  string trace_json = TraceResultBuffer::FlushTraceLogToString();
  ASSERT_EQ(1001, ParseAndReturnEventCount(trace_json));
}

// Regression test for KUDU-753: faulty JSON escaping when dealing with
// single quote characters.
TEST_F(TraceTest, TestJsonEncodingString) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 TraceLog::RECORD_CONTINUOUSLY);
  {
    TRACE_EVENT1("test", "test", "arg", "this is a test with \"'\"' and characters\nand new lines");
  }
  tl->SetDisabled();
  string trace_json = TraceResultBuffer::FlushTraceLogToString();
  ASSERT_EQ(1, ParseAndReturnEventCount(trace_json));
}

// Generate trace events continuously until 'latch' fires.
// Increment *num_events_generated for each event generated.
void GenerateTracesUntilLatch(AtomicInt<int64_t>* num_events_generated,
                              CountDownLatch* latch) {
  while (latch->count()) {
    {
      // This goes in its own scope so that the event is fully generated (with
      // both its START and END times) before we do the counter increment below.
      TRACE_EVENT0("test", "GenerateTracesUntilLatch");
    }
    num_events_generated->Increment();
  }
}

// Test starting and stopping tracing while a thread is running.
// This is a regression test for bugs in earlier versions of the imported
// trace code.
TEST_F(TraceTest, TestStartAndStopCollection) {
  TraceLog* tl = TraceLog::GetInstance();

  CountDownLatch latch(1);
  AtomicInt<int64_t> num_events_generated(0);
  scoped_refptr<Thread> t;
  CHECK_OK(Thread::CreateWithFlags(
      "test", "gen-traces",
      [&]() { GenerateTracesUntilLatch(&num_events_generated, &latch); },
      Thread::NO_STACK_WATCHDOG, &t));

  const int num_flushes = AllowSlowTests() ? 50 : 3;
  for (int i = 0; i < num_flushes; i++) {
    tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                   TraceLog::RECORDING_MODE,
                   TraceLog::RECORD_CONTINUOUSLY);

    const int64_t num_events_before = num_events_generated.Load();
    SleepFor(MonoDelta::FromMilliseconds(10));
    const int64_t num_events_after = num_events_generated.Load();
    tl->SetDisabled();

    string trace_json = TraceResultBuffer::FlushTraceLogToString();
    // We might under-count the number of events, since we only measure the sleep,
    // and tracing is enabled before and disabled after we start counting.
    // We might also over-count by at most 1, because we could enable tracing
    // right in between creating a trace event and incrementing the counter.
    // But, we should never over-count by more than 1.
    int expected_events_lowerbound = num_events_after - num_events_before - 1;
    int captured_events = ParseAndReturnEventCount(trace_json);
    ASSERT_GE(captured_events, expected_events_lowerbound);
  }

  latch.CountDown();
  t->Join();
}

TEST_F(TraceTest, TestChromeSampling) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 static_cast<TraceLog::Options>(TraceLog::RECORD_CONTINUOUSLY |
                                                TraceLog::ENABLE_SAMPLING));

  for (int i = 0; i < 100; i++) {
    switch (i % 3) {
      case 0:
        TRACE_EVENT_SET_SAMPLING_STATE("test", "state-0");
        break;
      case 1:
        TRACE_EVENT_SET_SAMPLING_STATE("test", "state-1");
        break;
      case 2:
        TRACE_EVENT_SET_SAMPLING_STATE("test", "state-2");
        break;
    }
    SleepFor(MonoDelta::FromMilliseconds(1));
  }
  tl->SetDisabled();
  string trace_json = TraceResultBuffer::FlushTraceLogToString();
  ASSERT_GT(ParseAndReturnEventCount(trace_json), 0);
}

class TraceEventCallbackTest : public KuduTest {
 public:
  virtual void SetUp() OVERRIDE {
    KuduTest::SetUp();
    ASSERT_EQ(nullptr, s_instance);
    s_instance = this;
  }
  virtual void TearDown() OVERRIDE {
    TraceLog::GetInstance()->SetDisabled();

    // Flush the buffer so that one test doesn't end up leaving any
    // extra results for the next test.
    TraceResultBuffer::FlushTraceLogToString();

    ASSERT_TRUE(!!s_instance);
    s_instance = nullptr;
    KuduTest::TearDown();

  }

 protected:
  void EndTraceAndFlush() {
    TraceLog::GetInstance()->SetDisabled();
    string trace_json = TraceResultBuffer::FlushTraceLogToString();
    trace_doc_.Parse<0>(trace_json.c_str());
    LOG(INFO) << trace_json;
    ASSERT_TRUE(trace_doc_.IsObject());
    trace_parsed_ = trace_doc_["traceEvents"];
    ASSERT_TRUE(trace_parsed_.IsArray());
  }

  void DropTracedMetadataRecords() {
    // NB: rapidjson has move-semantics, like auto_ptr.
    Value old_trace_parsed;
    old_trace_parsed = trace_parsed_;
    trace_parsed_.SetArray();
    size_t old_trace_parsed_size = old_trace_parsed.Size();

    for (size_t i = 0; i < old_trace_parsed_size; i++) {
      Value value;
      value = old_trace_parsed[i];
      if (value.GetType() != rapidjson::kObjectType) {
        trace_parsed_.PushBack(value, trace_doc_.GetAllocator());
        continue;
      }
      string tmp;
      if (value.HasMember("ph") && strcmp(value["ph"].GetString(), "M") == 0) {
        continue;
      }

      trace_parsed_.PushBack(value, trace_doc_.GetAllocator());
    }
  }

  // Search through the given array for any dictionary which has a key
  // or value which has 'string_to_match' as a substring.
  // Returns the matching dictionary, or NULL.
  static const Value* FindTraceEntry(
    const Value& trace_parsed,
    const char* string_to_match) {
    // Scan all items
    size_t trace_parsed_count = trace_parsed.Size();
    for (size_t i = 0; i < trace_parsed_count; i++) {
      const Value& value = trace_parsed[i];
      if (value.GetType() != rapidjson::kObjectType) {
        continue;
      }

      for (Value::ConstMemberIterator it = value.MemberBegin();
           it != value.MemberEnd();
           ++it) {
        if (it->name.IsString() && strstr(it->name.GetString(), string_to_match) != nullptr) {
          return &value;
        }
        if (it->value.IsString() && strstr(it->value.GetString(), string_to_match) != nullptr) {
          return &value;
        }
      }
    }
    return nullptr;
  }

  // For TraceEventCallbackAndRecordingX tests.
  void VerifyCallbackAndRecordedEvents(size_t expected_callback_count,
                                       size_t expected_recorded_count) {
    // Callback events.
    EXPECT_EQ(expected_callback_count, collected_events_names_.size());
    for (size_t i = 0; i < collected_events_names_.size(); ++i) {
      EXPECT_EQ("callback", collected_events_categories_[i]);
      EXPECT_EQ("yes", collected_events_names_[i]);
    }

    // Recorded events.
    EXPECT_EQ(expected_recorded_count, trace_parsed_.Size());
    EXPECT_TRUE(FindTraceEntry(trace_parsed_, "recording"));
    EXPECT_FALSE(FindTraceEntry(trace_parsed_, "callback"));
    EXPECT_TRUE(FindTraceEntry(trace_parsed_, "yes"));
    EXPECT_FALSE(FindTraceEntry(trace_parsed_, "no"));
  }

  void VerifyCollectedEvent(size_t i,
                            unsigned phase,
                            const string& category,
                            const string& name) {
    EXPECT_EQ(phase, collected_events_phases_[i]);
    EXPECT_EQ(category, collected_events_categories_[i]);
    EXPECT_EQ(name, collected_events_names_[i]);
  }

  Document trace_doc_;
  Value trace_parsed_;

  vector<string> collected_events_categories_;
  vector<string> collected_events_names_;
  vector<unsigned char> collected_events_phases_;
  vector<MicrosecondsInt64> collected_events_timestamps_;

  static TraceEventCallbackTest* s_instance;
  static void Callback(MicrosecondsInt64 timestamp,
                       char phase,
                       const unsigned char* category_group_enabled,
                       const char* name,
                       uint64_t id,
                       int num_args,
                       const char* const arg_names[],
                       const unsigned char arg_types[],
                       const uint64_t arg_values[],
                       unsigned char flags) {
    s_instance->collected_events_phases_.push_back(phase);
    s_instance->collected_events_categories_.emplace_back(
        TraceLog::GetCategoryGroupName(category_group_enabled));
    s_instance->collected_events_names_.emplace_back(name);
    s_instance->collected_events_timestamps_.push_back(timestamp);
  }
};

TraceEventCallbackTest* TraceEventCallbackTest::s_instance;

TEST_F(TraceEventCallbackTest, TraceEventCallback) {
  TRACE_EVENT_INSTANT0("all", "before enable", TRACE_EVENT_SCOPE_THREAD);
  TraceLog::GetInstance()->SetEventCallbackEnabled(
      CategoryFilter("*"), Callback);
  TRACE_EVENT_INSTANT0("all", "event1", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("all", "event2", TRACE_EVENT_SCOPE_GLOBAL);
  {
    TRACE_EVENT0("all", "duration");
    TRACE_EVENT_INSTANT0("all", "event3", TRACE_EVENT_SCOPE_GLOBAL);
  }
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0("all", "after callback removed",
                       TRACE_EVENT_SCOPE_GLOBAL);
  ASSERT_EQ(5u, collected_events_names_.size());
  EXPECT_EQ("event1", collected_events_names_[0]);
  EXPECT_EQ(TRACE_EVENT_PHASE_INSTANT, collected_events_phases_[0]);
  EXPECT_EQ("event2", collected_events_names_[1]);
  EXPECT_EQ(TRACE_EVENT_PHASE_INSTANT, collected_events_phases_[1]);
  EXPECT_EQ("duration", collected_events_names_[2]);
  EXPECT_EQ(TRACE_EVENT_PHASE_BEGIN, collected_events_phases_[2]);
  EXPECT_EQ("event3", collected_events_names_[3]);
  EXPECT_EQ(TRACE_EVENT_PHASE_INSTANT, collected_events_phases_[3]);
  EXPECT_EQ("duration", collected_events_names_[4]);
  EXPECT_EQ(TRACE_EVENT_PHASE_END, collected_events_phases_[4]);
  for (size_t i = 1; i < collected_events_timestamps_.size(); i++) {
    EXPECT_LE(collected_events_timestamps_[i - 1],
              collected_events_timestamps_[i]);
  }
}

TEST_F(TraceEventCallbackTest, TraceEventCallbackWhileFull) {
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("*"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  do {
    TRACE_EVENT_INSTANT0("all", "badger badger", TRACE_EVENT_SCOPE_GLOBAL);
  } while (!TraceLog::GetInstance()->BufferIsFull());
  TraceLog::GetInstance()->SetEventCallbackEnabled(CategoryFilter("*"),
                                                   Callback);
  TRACE_EVENT_INSTANT0("all", "a snake", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  ASSERT_EQ(1u, collected_events_names_.size());
  EXPECT_EQ("a snake", collected_events_names_[0]);
}

// 1: Enable callback, enable recording, disable callback, disable recording.
TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecording1) {
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackEnabled(CategoryFilter("callback"),
                                                   Callback);
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("recording"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  EndTraceAndFlush();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);

  DropTracedMetadataRecords();
  NO_FATALS();
  VerifyCallbackAndRecordedEvents(2, 2);
}

// 2: Enable callback, enable recording, disable recording, disable callback.
TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecording2) {
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackEnabled(CategoryFilter("callback"),
                                                   Callback);
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("recording"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  EndTraceAndFlush();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);

  DropTracedMetadataRecords();
  VerifyCallbackAndRecordedEvents(3, 1);
}

// 3: Enable recording, enable callback, disable callback, disable recording.
TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecording3) {
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("recording"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackEnabled(CategoryFilter("callback"),
                                                   Callback);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  EndTraceAndFlush();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);

  DropTracedMetadataRecords();
  VerifyCallbackAndRecordedEvents(1, 3);
}

// 4: Enable recording, enable callback, disable recording, disable callback.
TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecording4) {
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter("recording"),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_UNTIL_FULL);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackEnabled(CategoryFilter("callback"),
                                                   Callback);
  TRACE_EVENT_INSTANT0("recording", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  EndTraceAndFlush();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "yes", TRACE_EVENT_SCOPE_GLOBAL);
  TraceLog::GetInstance()->SetEventCallbackDisabled();
  TRACE_EVENT_INSTANT0("recording", "no", TRACE_EVENT_SCOPE_GLOBAL);
  TRACE_EVENT_INSTANT0("callback", "no", TRACE_EVENT_SCOPE_GLOBAL);

  DropTracedMetadataRecords();
  VerifyCallbackAndRecordedEvents(2, 2);
}

TEST_F(TraceEventCallbackTest, TraceEventCallbackAndRecordingDuration) {
  TraceLog::GetInstance()->SetEventCallbackEnabled(CategoryFilter("*"),
                                                   Callback);
  {
    TRACE_EVENT0("callback", "duration1");
    TraceLog::GetInstance()->SetEnabled(
        CategoryFilter("*"),
        TraceLog::RECORDING_MODE,
        TraceLog::RECORD_UNTIL_FULL);
    TRACE_EVENT0("callback", "duration2");
    EndTraceAndFlush();
    TRACE_EVENT0("callback", "duration3");
  }
  TraceLog::GetInstance()->SetEventCallbackDisabled();

  ASSERT_EQ(6u, collected_events_names_.size());
  VerifyCollectedEvent(0, TRACE_EVENT_PHASE_BEGIN, "callback", "duration1");
  VerifyCollectedEvent(1, TRACE_EVENT_PHASE_BEGIN, "callback", "duration2");
  VerifyCollectedEvent(2, TRACE_EVENT_PHASE_BEGIN, "callback", "duration3");
  VerifyCollectedEvent(3, TRACE_EVENT_PHASE_END, "callback", "duration3");
  VerifyCollectedEvent(4, TRACE_EVENT_PHASE_END, "callback", "duration2");
  VerifyCollectedEvent(5, TRACE_EVENT_PHASE_END, "callback", "duration1");
}

////////////////////////////////////////////////////////////
// Tests for synthetic delay
// (from chromium-base/debug/trace_event_synthetic_delay_unittest.cc)
////////////////////////////////////////////////////////////

namespace {

const int kTargetDurationMs = 100;
// Allow some leeway in timings to make it possible to run these tests with a
// wall clock time source too.
const int kShortDurationMs = 10;

}  // namespace

namespace debug {

class TraceEventSyntheticDelayTest : public KuduTest,
                                     public TraceEventSyntheticDelayClock {
 public:
  TraceEventSyntheticDelayTest() {
    now_ = MonoTime::Min();
  }

  virtual ~TraceEventSyntheticDelayTest() {
    ResetTraceEventSyntheticDelays();
  }

  // TraceEventSyntheticDelayClock implementation.
  virtual MonoTime Now() OVERRIDE {
    AdvanceTime(MonoDelta::FromMilliseconds(kShortDurationMs / 10));
    return now_;
  }

  TraceEventSyntheticDelay* ConfigureDelay(const char* name) {
    TraceEventSyntheticDelay* delay = TraceEventSyntheticDelay::Lookup(name);
    delay->SetClock(this);
    delay->SetTargetDuration(
      MonoDelta::FromMilliseconds(kTargetDurationMs));
    return delay;
  }

  void AdvanceTime(MonoDelta delta) { now_ += delta; }

  int TestFunction() {
    MonoTime start = Now();
    { TRACE_EVENT_SYNTHETIC_DELAY("test.Delay"); }
    MonoTime end = Now();
    return (end - start).ToMilliseconds();
  }

  int AsyncTestFunctionBegin() {
    MonoTime start = Now();
    { TRACE_EVENT_SYNTHETIC_DELAY_BEGIN("test.AsyncDelay"); }
    MonoTime end = Now();
    return (end - start).ToMilliseconds();
  }

  int AsyncTestFunctionEnd() {
    MonoTime start = Now();
    { TRACE_EVENT_SYNTHETIC_DELAY_END("test.AsyncDelay"); }
    MonoTime end = Now();
    return (end - start).ToMilliseconds();
  }

 private:
  MonoTime now_;

  DISALLOW_COPY_AND_ASSIGN(TraceEventSyntheticDelayTest);
};

TEST_F(TraceEventSyntheticDelayTest, StaticDelay) {
  TraceEventSyntheticDelay* delay = ConfigureDelay("test.Delay");
  delay->SetMode(TraceEventSyntheticDelay::STATIC);
  EXPECT_GE(TestFunction(), kTargetDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, OneShotDelay) {
  TraceEventSyntheticDelay* delay = ConfigureDelay("test.Delay");
  delay->SetMode(TraceEventSyntheticDelay::ONE_SHOT);
  EXPECT_GE(TestFunction(), kTargetDurationMs);
  EXPECT_LT(TestFunction(), kShortDurationMs);

  delay->SetTargetDuration(
      MonoDelta::FromMilliseconds(kTargetDurationMs));
  EXPECT_GE(TestFunction(), kTargetDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AlternatingDelay) {
  TraceEventSyntheticDelay* delay = ConfigureDelay("test.Delay");
  delay->SetMode(TraceEventSyntheticDelay::ALTERNATING);
  EXPECT_GE(TestFunction(), kTargetDurationMs);
  EXPECT_LT(TestFunction(), kShortDurationMs);
  EXPECT_GE(TestFunction(), kTargetDurationMs);
  EXPECT_LT(TestFunction(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelay) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_GE(AsyncTestFunctionEnd(), kTargetDurationMs / 2);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayExceeded) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  AdvanceTime(MonoDelta::FromMilliseconds(kTargetDurationMs));
  EXPECT_LT(AsyncTestFunctionEnd(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayNoActivation) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionEnd(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayNested) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_LT(AsyncTestFunctionEnd(), kShortDurationMs);
  EXPECT_GE(AsyncTestFunctionEnd(), kTargetDurationMs / 2);
}

TEST_F(TraceEventSyntheticDelayTest, AsyncDelayUnbalanced) {
  ConfigureDelay("test.AsyncDelay");
  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_GE(AsyncTestFunctionEnd(), kTargetDurationMs / 2);
  EXPECT_LT(AsyncTestFunctionEnd(), kShortDurationMs);

  EXPECT_LT(AsyncTestFunctionBegin(), kShortDurationMs);
  EXPECT_GE(AsyncTestFunctionEnd(), kTargetDurationMs / 2);
}

TEST_F(TraceEventSyntheticDelayTest, ResetDelays) {
  ConfigureDelay("test.Delay");
  ResetTraceEventSyntheticDelays();
  EXPECT_LT(TestFunction(), kShortDurationMs);
}

TEST_F(TraceEventSyntheticDelayTest, BeginParallel) {
  TraceEventSyntheticDelay* delay = ConfigureDelay("test.AsyncDelay");
  MonoTime end_times[2];
  MonoTime start_time = Now();

  delay->BeginParallel(&end_times[0]);
  EXPECT_FALSE(!end_times[0].Initialized());

  delay->BeginParallel(&end_times[1]);
  EXPECT_FALSE(!end_times[1].Initialized());

  delay->EndParallel(end_times[0]);
  EXPECT_GE((Now() - start_time).ToMilliseconds(), kTargetDurationMs);

  start_time = Now();
  delay->EndParallel(end_times[1]);
  EXPECT_LT((Now() - start_time).ToMilliseconds(), kShortDurationMs);
}

TEST_F(TraceTest, TestVLogTrace) {
  for (FLAGS_v = 0; FLAGS_v <= 1; FLAGS_v++) {
    TraceLog* tl = TraceLog::GetInstance();
    tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                   TraceLog::RECORDING_MODE,
                   TraceLog::RECORD_CONTINUOUSLY);
    VLOG_AND_TRACE("test", 1) << "hello world";
    tl->SetDisabled();
    string trace_json = TraceResultBuffer::FlushTraceLogToString();
    ASSERT_STR_CONTAINS(trace_json, "hello world");
    ASSERT_STR_CONTAINS(trace_json, "trace-test.cc");
  }
}

namespace {
string FunctionWithSideEffect(bool* b) {
  *b = true;
  return "function-result";
}
} // anonymous namespace

// Test that, if tracing is not enabled, a VLOG_AND_TRACE doesn't evaluate its
// arguments.
TEST_F(TraceTest, TestVLogTraceLazyEvaluation) {
  FLAGS_v = 0;
  bool function_run = false;
  VLOG_AND_TRACE("test", 1) << FunctionWithSideEffect(&function_run);
  ASSERT_FALSE(function_run);

  // If we enable verbose logging, we should run the side effect even though
  // trace logging is disabled.
  FLAGS_v = 1;
  VLOG_AND_TRACE("test", 1) << FunctionWithSideEffect(&function_run);
  ASSERT_TRUE(function_run);
}

TEST_F(TraceTest, TestVLogAndEchoToConsole) {
  TraceLog* tl = TraceLog::GetInstance();
  tl->SetEnabled(CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
                 TraceLog::RECORDING_MODE,
                 TraceLog::ECHO_TO_CONSOLE);
  FLAGS_v = 1;
  VLOG_AND_TRACE("test", 1) << "hello world";
  tl->SetDisabled();
}

TEST_F(TraceTest, TestTraceMetrics) {
  scoped_refptr<Trace> trace(new Trace);
  trace->metrics()->Increment("foo", 10);
  trace->metrics()->Increment("bar", 10);
  for (int i = 0; i < 1000; i++) {
    trace->metrics()->Increment("baz", i);
  }
  EXPECT_EQ("{\"bar\":10,\"baz\":499500,\"foo\":10}",
            trace->MetricsAsJSON());

  {
    ADOPT_TRACE(trace.get());
    TRACE_COUNTER_SCOPE_LATENCY_US("test_scope_us");
    SleepFor(MonoDelta::FromMilliseconds(100));
  }
  auto m = trace->metrics()->Get();
  EXPECT_GE(m["test_scope_us"], 80 * 1000);
}

// Regression test for KUDU-2075: using tracing from vanilla threads
// should work fine, even if some pthread_self identifiers have been
// reused.
TEST_F(TraceTest, TestTraceFromVanillaThreads) {
  TraceLog::GetInstance()->SetEnabled(
      CategoryFilter(CategoryFilter::kDefaultCategoryFilterString),
      TraceLog::RECORDING_MODE,
      TraceLog::RECORD_CONTINUOUSLY);
  SCOPED_CLEANUP({ TraceLog::GetInstance()->SetDisabled(); });

  // Do several passes to make it more likely that the thread identifiers
  // will get reused.
  for (int pass = 0; pass < 10; pass++) {
    vector<thread> threads;
    for (int i = 0; i < 100; i++) {
      threads.emplace_back([i] {
          GenerateTraceEvents(i, 1);
        });
    }
    for (auto& t : threads) {
      t.join();
    }
  }
}
} // namespace debug
} // namespace kudu
