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

// Most code of this file is copied and modified from rocksdb SyncPoint.
// And modified by Gavin (github.com/gavinchou).

#pragma once
// clang-format off
#include <functional>
#include <mutex>
#include <string>
#include <vector>

namespace doris::cloud {

// This class provides facility to reproduce race conditions deterministically
// in unit tests.
// Developer could specify sync points in the codebase via TEST_SYNC_POINT.
// Each sync point represents a position in the execution stream of a thread.
// In the unit test, 'Happens After' relationship among sync points could be
// setup via SyncPoint::load_dependency, to reproduce a desired interleave of
// threads execution.
// Refer to (DBTest,TransactionLogIteratorRace), for an example use case.
class SyncPoint {
public:
  static SyncPoint* get_instance();
  SyncPoint(const SyncPoint&) = delete;
  SyncPoint& operator=(const SyncPoint&) = delete;
  ~SyncPoint();
  struct SyncPointPair {
    std::string predecessor;
    std::string successor;
  };

  // call once at the beginning of a test to setup the dependency between
  // sync points
  //
  // Example:
  // load_dependency({{"point1", "point2"},
  //                  {"point2", "point3"},
  //                  {"point3", "point4"}});
  //
  //    test case thread            thread for object being tested
  //        |                                  |
  //        |                                  |
  //        | \-------------0-------------\    |
  //        |                              \-> x  sync point1 set in code
  //        |    /----------1----------------/ |
  // point2 o <-/                          /-> x  sync point4 set in code
  //        |                             /    |
  //        z                            /     |
  //        z     /---------2-----------/      |  there may be nothing
  //        |    /                             |  between point1 point4
  // point3 o --/                              |  they are for sync
  //        |                                  |  between test case and object
  //        v                                  v
  //
  // vertical arrow means the procedure of each thread, the running order will
  // be:
  // test case thread -> point1 -> point2 -> point3 -> point4 -> object being
  // tested
  //
  // we may do a lot of things between point2 and point3, say, change the
  // object's status, call another method, propagate data race and etc.
  void load_dependency(const std::vector<SyncPointPair>& dependencies);

  // call once at the beginning of a test to setup the dependency between
  // sync points and setup markers indicating the successor is only enabled
  // when it is processed on the same thread as the predecessor.
  // When adding a marker, it implicitly adds a dependency for the marker pair.
  void load_dependency_and_markers(
                                const std::vector<SyncPointPair>& dependencies,
                                const std::vector<SyncPointPair>& markers);

  // The argument to the callback is passed through from
  // TEST_SYNC_POINT_CALLBACK(); nullptr if TEST_SYNC_POINT or
  // TEST_IDX_SYNC_POINT was used.
  void set_call_back(const std::string& point,
                     const std::function<void(void*)>& callback);

  // Clear callback function by point
  void clear_call_back(const std::string& point);

  // Clear all call back functions.
  void clear_all_call_backs();

  // Enable sync point processing (disabled on startup)
  void enable_processing();

  // Disable sync point processing
  void disable_processing();

  // Remove the execution trace of all sync points
  void clear_trace();

  // Triggered by TEST_SYNC_POINT, blocking execution until all predecessors
  // are executed.
  // And/or call registered callback function, with argument `cb_arg`
  void process(const std::string& point, void* cb_arg = nullptr);

  // TODO: it might be useful to provide a function that blocks until all
  //       sync points are cleared.
  // We want this to be public so we can subclass the implementation
  struct Data;

private:
   // Singleton
  SyncPoint();
  Data* impl_; // implementation which is hidden in cpp file
};
} // namespace doris::cloud

// TEST_SYNC_POINT is no op in release build.
// Turn on this feature by defining the macro
#ifndef UNIT_TEST
# define TEST_SYNC_POINT(x)
# define TEST_IDX_SYNC_POINT(x, index)
# define TEST_SYNC_POINT_CALLBACK(x, y)
# define TEST_SYNC_POINT_RETURN_WITH_VALUE(sync_point_name, ret_val_ptr)
# define TEST_SYNC_POINT_RETURN_WITH_VOID(sync_point_name)
// seldom called
# define INIT_SYNC_POINT_SINGLETONS()
#else
// Use TEST_SYNC_POINT to specify sync points inside code base.
// Sync points can have happens-after depedency on other sync points,
// configured at runtime via SyncPoint::load_dependency. This could be
// utilized to re-produce race conditions between threads.
# define TEST_SYNC_POINT(x) doris::cloud::SyncPoint::get_instance()->process(x)
# define TEST_IDX_SYNC_POINT(x, index) \
  doris::cloud::SyncPoint::get_instance()->process(x + std::to_string(index))
# define TEST_SYNC_POINT_CALLBACK(x, y) \
  doris::cloud::SyncPoint::get_instance()->process(x, y)
# define INIT_SYNC_POINT_SINGLETONS() \
  (void)doris::cloud::SyncPoint::get_instance();

/**
 * Inject return points for testing.
 *
 * Currently we can only insert more points to get context from tested thread
 * and process in testing thread, e.g.
 *
 * tested thread:
 * ...
 * TEST_SYNC_POINT_CALLBACK("point_ctx", ptr_to_ctx);
 * TEST_SYNC_POINT_RETURN_WITH_VALUE("point_ret", ptr_to_ret_val);
 * ...
 *
 * testing thread:
 * sync_point->add("point_ctx", [&ctx](void* ptr_to_ctx) { ctx = ptr_to_ctx; });
 * sync_point->add("point_ret", [](void* ptr_to_ret) {...});
 * sync_point->add("point_ret::pred", [&ctx](void* pred) { pred = *ctx ? true : false; });
 *
 * See sync_point_test.cpp for more details.
 */
#pragma GCC diagnostic ignored "-Waddress"
# define TEST_SYNC_POINT_RETURN_WITH_VALUE(sync_point_name, ret_val_ptr) \
static_assert(ret_val_ptr != nullptr, "ret_val_ptr cannot be nullptr");\
TEST_SYNC_POINT_CALLBACK(sync_point_name, ret_val_ptr); \
{ \
  bool pred = false; \
  TEST_SYNC_POINT_CALLBACK(std::string(sync_point_name) + "::pred", &pred); \
  if (pred) return *ret_val_ptr; \
}

# define TEST_SYNC_POINT_RETURN_WITH_VOID(sync_point_name) \
{ \
  bool pred = false; \
  TEST_SYNC_POINT_CALLBACK(std::string(sync_point_name) + "::pred", &pred); \
  if (pred) return; \
}

#endif // UNIT_TEST

// TODO: define injection point in production env.
//       the `if` expr can be live configure of the application
#define ENABLE_INJECTION_POINT 0
#ifndef ENABLE_INJECTION_POINT
# define TEST_INJECTION_POINT(x)
# define TEST_IDX_TEST_INJECTION_POINT(x, index)
# define TEST_INJECTION_POINT_CALLBACK(x, y)
# define INIT_INJECTION_POINT_SINGLETONS()
#else
# define TEST_INJECTION_POINT(x)                                                    \
  if (ENABLE_INJECTION_POINT) {                                                     \
    doris::cloud::SyncPoint::get_instance()->process(x);                            \
  }
# define TEST_IDX_INJECTION_POINT(x, index)                                         \
  if (ENABLE_INJECTION_POINT) {                                                     \
    doris::cloud::SyncPoint::get_instance()->process(x + std::to_string(index));    \
  }
# define TEST_INJECTION_POINT_CALLBACK(x, y)                                        \
  if (ENABLE_INJECTION_POINT) {                                                     \
    doris::cloud::SyncPoint::get_instance()->process(x, y);                         \
  }
# define INIT_INJECTION_POINT_SINGLETONS()                                          \
  if (ENABLE_INJECTION_POINT) {                                                     \
    (void)doris::cloud::SyncPoint::get_instance();                                  \
  }
#endif // ENABLE_INJECTION_POINT

// clang-format on
