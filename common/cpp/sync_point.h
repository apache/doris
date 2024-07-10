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

// Most code of this file is copied from rocksdb SyncPoint.
// https://github.com/facebook/rocksdb

#pragma once
// clang-format off
#include <functional>
#include <iostream>
#include <string>
#include <vector>
#include <any>

namespace doris {

#define SYNC_POINT_HOOK_RETURN_VALUE(expr, point_name, ...)                              \
    [&]() mutable {                                                                      \
        TEST_SYNC_POINT_RETURN_WITH_VALUE(point_name, decltype((expr)) {}, __VA_ARGS__); \
        return (expr);                                                                   \
    }()

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
  // ponit3 o --/                              |  they are for sync
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

  class CallbackGuard {
  public:
    CallbackGuard() = default;
    explicit CallbackGuard(std::string point) : _point(std::move(point)) {}
    ~CallbackGuard() {
      if (!_point.empty()) {
        get_instance()->clear_call_back(_point);
      }
    }
    CallbackGuard(const CallbackGuard&) = delete;
    CallbackGuard& operator=(const CallbackGuard&) = delete;

    CallbackGuard(CallbackGuard&& other) noexcept {
      if (!_point.empty() && _point != other._point) {
        get_instance()->clear_call_back(_point);
      }
      _point = std::move(other._point);
    }

    CallbackGuard& operator=(CallbackGuard&& other) noexcept {
      if (!_point.empty() && _point != other._point) {
        get_instance()->clear_call_back(_point);
      }
      _point = std::move(other._point);
      return *this;
    };

  private:
    std::string _point;
  };

  // The argument to the callback is passed through from
  // TEST_SYNC_POINT_CALLBACK(); nullptr if TEST_SYNC_POINT or
  // TEST_IDX_SYNC_POINT was used.
  // If `guard` is not nullptr, method will return a `CallbackGuard` object which will clear the
  // callback when it is destructed.
  void set_call_back(const std::string& point,
                     const std::function<void(std::vector<std::any>&&)>& callback,
                     CallbackGuard* guard = nullptr);

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
  // And/or call registered callback function, with argument `cb_args`
  void process(const std::string& point, std::vector<std::any>&& cb_args = {});

  // TODO: it might be useful to provide a function that blocks until all
  //       sync points are cleared.
  // We want this to be public so we can subclass the implementation
  struct Data;

private:
   // Singleton
  SyncPoint();
  Data* impl_; // impletation which is hidden in cpp file
};

template <class T>
T try_any_cast(const std::any& a) {
  try {
    return std::any_cast<T>(a);
  } catch (const std::bad_any_cast& e) { 
    std::cerr << e.what() << " expected=" << typeid(T).name() << " actual=" << a.type().name() << std::endl;
    throw e;
  }
}

template <typename T>
auto try_any_cast_ret(std::vector<std::any>& any) {
    return try_any_cast<std::pair<T, bool>*>(any.back());
}

} // namespace doris

#define SYNC_POINT(x) doris::SyncPoint::get_instance()->process(x)
#define IDX_SYNC_POINT(x, index) \
    doris::SyncPoint::get_instance()->process(x + std::to_string(index))
#define SYNC_POINT_CALLBACK(x, ...) doris::SyncPoint::get_instance()->process(x, {__VA_ARGS__})
#define SYNC_POINT_RETURN_WITH_VALUE(x, default_ret_val, ...) \
{ \
  std::pair ret {default_ret_val, false}; \
  std::vector<std::any> args {__VA_ARGS__}; \
  args.emplace_back(&ret); \
  doris::SyncPoint::get_instance()->process(x, std::move(args)); \
  if (ret.second) return std::move(ret.first); \
}
#define SYNC_POINT_RETURN_WITH_VOID(x, ...) \
{ \
  bool pred = false; \
  std::vector<std::any> args {__VA_ARGS__}; \
  args.emplace_back(&pred); \
  doris::SyncPoint::get_instance()->process(x, std::move(args)); \
  if (pred) return; \
}
#define SYNC_POINT_SINGLETON() (void)doris::SyncPoint::get_instance() 

// TEST_SYNC_POINT is no op in release build.
// Turn on this feature by defining the macro
#ifndef BE_TEST
# define TEST_SYNC_POINT(x)
# define TEST_IDX_SYNC_POINT(x, index)
# define TEST_SYNC_POINT_CALLBACK(x, ...)
# define TEST_SYNC_POINT_RETURN_WITH_VALUE(x, default_ret_val, ...)
# define TEST_SYNC_POINT_RETURN_WITH_VOID(x, ...)
// seldom called
# define TEST_SYNC_POINT_SINGLETON()
#else
// Use TEST_SYNC_POINT to specify sync points inside code base.
// Sync points can have happens-after depedency on other sync points,
// configured at runtime via SyncPoint::load_dependency. This could be
// utilized to re-produce race conditions between threads.
# define TEST_SYNC_POINT(x) SYNC_POINT(x)
# define TEST_IDX_SYNC_POINT(x, index) IDX_SYNC_POINT(x, index)
# define TEST_SYNC_POINT_CALLBACK(x, ...) SYNC_POINT_CALLBACK(x, __VA_ARGS__)
# define TEST_SYNC_POINT_SINGLETON() SYNC_POINT_SINGLETON()

/**
 * Inject return points for testing.
 *
 * Currently we can only insert more points to get context from tested thread
 * and process in testing thread, e.g.
 *
 * tested thread:
 * ...
 * TEST_SYNC_POINT_RETURN_WITH_VALUE("point_ret", int(0), ctx0);
 * ...
 *
 * testing thread:
 * sync_point->add("point_ret", [](auto&& args) {
 *     auto ctx0 = try_any_cast<bool>(args[0]);
 *     auto pair = try_any_cast<std::pair<int, bool>*>(args.back());
 *     pair->first = ...;
 *     pair->second = ctx0; });
 *
 * See sync_piont_test.cpp for more details.
 */
#pragma GCC diagnostic ignored "-Waddress"
# define TEST_SYNC_POINT_RETURN_WITH_VALUE(x, default_ret_val, ...) SYNC_POINT_RETURN_WITH_VALUE(x, default_ret_val, __VA_ARGS__)
# define TEST_SYNC_POINT_RETURN_WITH_VOID(x, ...) SYNC_POINT_RETURN_WITH_VOID(x, __VA_ARGS__)

#endif // BE_TEST

// TODO: define injection point in production env.
//       the `if` expr can be live configure of the application
#ifndef ENABLE_INJECTION_POINT
# define TEST_INJECTION_POINT(x)
# define TEST_IDX_INJECTION_POINT(x, index)
# define TEST_INJECTION_POINT_CALLBACK(x, ...)
# define TEST_INJECTION_POINT_RETURN_WITH_VALUE(x, default_ret_val, ...)
# define TEST_INJECTION_POINT_RETURN_WITH_VOID(x, ...)
# define TEST_INJECTION_POINT_SINGLETON()
#else
# define TEST_INJECTION_POINT(x) SYNC_POINT(x);
# define TEST_IDX_INJECTION_POINT(x, index) IDX_SYNC_POINT(x, index);
# define TEST_INJECTION_POINT_CALLBACK(x, ...) SYNC_POINT_CALLBACK(x, __VA_ARGS__);
# define TEST_INJECTION_POINT_SINGLETON() SYNC_POINT_SINGLETON();
# define TEST_INJECTION_POINT_RETURN_WITH_VALUE(x, default_ret_val, ...) SYNC_POINT_RETURN_WITH_VALUE(x, default_ret_val, __VA_ARGS__);
# define TEST_INJECTION_POINT_RETURN_WITH_VOID(x, ...) SYNC_POINT_RETURN_WITH_VOID(x, __VA_ARGS__);
#endif // ENABLE_INJECTION_POINT

// clang-format on
// vim: et tw=80 ts=2 sw=2 cc=80:
