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

// switch on syncpoint macro
#ifndef UNIT_TEST
#define UNIT_TEST
#endif
#include "cpp/sync_point.h"

#include <cassert>
#include <condition_variable>
#include <iostream>
#include <thread>

#define STDOUT (std::cout << __PRETTY_FUNCTION__ << " ")

int g_data = 12315;

void foo() {
    int a = 10086;

    STDOUT << "a: " << a << std::endl;

    TEST_SYNC_POINT_CALLBACK("foo:a_assigned", &a); // a may be accessed

    STDOUT << "a: " << a << std::endl;

    int b = 100;

    STDOUT << "b: " << b << std::endl;
    TEST_SYNC_POINT_CALLBACK("foo:b_assigned", &b); // b may be accessed

    STDOUT << "b: " << b << std::endl;

    a += b;

    STDOUT << "a: " << a << std::endl;

    if (a == 10086 + 100) {
        STDOUT << "expected branch taken, a: " << a << std::endl;
    } else {
        STDOUT << "exceptional branch taken, a: " << a << std::endl;
    }
}

void test_foo_single_thread() {
    auto sp = doris::SyncPoint::get_instance();
    std::cout << "========== default behavior ==========" << std::endl;
    foo(); // first run, nothing changed

    std::cout << "========== change default behavior ==========" << std::endl;
    sp->set_call_back("foo:a_assigned", [](void* arg) {
        int& a = *reinterpret_cast<int*>(arg); // we known what arg is
        STDOUT << "original a: " << a << std::endl;
        a = 10010;
        STDOUT << "change a: " << a << std::endl;
    });

    sp->set_call_back("foo:b_assigned", [](void* arg) {
        int& b = *reinterpret_cast<int*>(arg); // we known what arg is
        STDOUT << "original b: " << b << std::endl;
        b = 200;
        STDOUT << "change b: " << b << std::endl;
    });

    sp->enable_processing(); // turn on
    foo();

    sp->clear_all_call_backs();
    sp->disable_processing();
    sp->clear_trace();
}

void foo(int x) {
    // multi-thread data race
    g_data = x;

    TEST_SYNC_POINT("foo:assigned:1");
    // concurrent stuff may be executed by other thread here,
    // and this thread may race with it
    TEST_SYNC_POINT("foo:assigned:2");

    STDOUT << x << std::endl;
}

void bar() {
    TEST_SYNC_POINT("bar:assigned:1");
    g_data = 10086;
    TEST_SYNC_POINT("bar:assigned:2");
}

void test_foo_data_race() {
    std::cout << "========== data race ==========" << std::endl;

    std::mutex mtx;
    std::condition_variable cv;
    auto sp = doris::SyncPoint::get_instance();

    bool go = false;
    std::unique_lock<std::mutex> lk(mtx);
    g_data = 0;

    // ===========================================================================
    // FORCE threads concurrent execution with sequence:
    //
    //      thread1       thread2       thread3
    //         |             |             |
    //         |             |           foo(3)
    //         |             |             |
    //         |           foo(2)          |
    //         |             |             |
    //       foo(1)          |             |
    //         |             |             |
    //         v             v             v
    //
    std::thread th1([&] {
        {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&] { return go; });
        }
        TEST_SYNC_POINT("test_foo_data_race:1:start");
        assert(g_data == 2);
        foo(1);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(g_data == 1);
        TEST_SYNC_POINT("test_foo_data_race:1:end");
    });
    std::thread th2([&] {
        {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&] { return go; });
        }
        TEST_SYNC_POINT("test_foo_data_race:2:start");
        assert(g_data == 3);
        foo(2);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(g_data == 2);
        TEST_SYNC_POINT("test_foo_data_race:2:end");
    });
    std::thread th3([&] {
        {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&] { return go; });
        }
        TEST_SYNC_POINT("test_foo_data_race:3:start");
        assert(g_data == 0);
        foo(3);
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        assert(g_data == 3);
        TEST_SYNC_POINT("test_foo_data_race:3:end");
    });

    // prepare dependency
    sp->enable_processing();
    // run foo(int) in sequence 3->2->1
    sp->load_dependency({
            {"test_foo_data_race:3:end", "test_foo_data_race:2:start"},
            {"test_foo_data_race:2:end", "test_foo_data_race:1:start"},
    });

    // set and go
    go = true;
    lk.unlock();
    cv.notify_all();

    th1.join();
    th2.join();
    th3.join();
    sp->clear_all_call_backs();
    sp->clear_trace();
    sp->disable_processing();

    // ===========================================================================
    // FORCE to run bar() in the middle of foo()
    //
    //   thread4            thread5
    //      |                  |
    //      |             foo():assigned:1
    //      |                  |
    //     bar()               |
    //      |                  |
    //      |             foo():assigned:2
    //      |                  |
    //      v                  v
    //
    lk.lock();
    go = false;

    std::thread th4([&] {
        {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&] { return go; });
        }
        bar();
    });
    std::thread th5([&] {
        {
            std::unique_lock<std::mutex> lk(mtx);
            cv.wait(lk, [&] { return go; });
        }
        foo(10010);              // try to set g_data to 10010
        assert(g_data == 10086); // foo() is racing with bar()
    });

    // prepare dependency
    sp->enable_processing();
    sp->load_dependency({
            {"foo:assigned:1", "bar:assigned:1"}, // no need to specify bar1->bar2,
            {"bar:assigned:2", "foo:assigned:2"}, // because they a natually sequenced
    });

    // set and go
    go = true;
    lk.unlock();
    cv.notify_all();

    th4.join();
    th5.join();
    sp->clear_all_call_backs();
    sp->clear_trace();

    sp->disable_processing();
}

int foo_return_with_value() {
    int ctx = 1;
    (void)ctx;
    // for those return values are not explictly declared
    {
        // for ctx capture
        TEST_SYNC_POINT_CALLBACK("foo_return_with_value1_ctx", &ctx);

        int tmp_ret = -1;
        (void)tmp_ret; // supress `unused` warning when build in release mode
        TEST_SYNC_POINT_RETURN_WITH_VALUE("foo_return_with_value1", &tmp_ret);
    }

    // for those return valuse are explicitly declared
    {
        int ret = -1;
        TEST_SYNC_POINT_RETURN_WITH_VALUE("foo_return_with_value2", &ret);
        return ret;
    }
}

void foo_return_with_void(int* in) {
    *in = 10000;
    TEST_SYNC_POINT_RETURN_WITH_VOID("foo_return_with_void1");
    *in = 10010;
    TEST_SYNC_POINT_RETURN_WITH_VOID("foo_return_with_void2");
    *in = 10086;
}

void test_return_point() {
    auto sp = doris::SyncPoint::get_instance();
    sp->clear_all_call_backs();
    sp->enable_processing();

    // test pred == false, nothing happens
    {
        sp->clear_all_call_backs();

        sp->set_call_back("foo_return_with_value1",
                          [](void* ret) { *reinterpret_cast<int*>(ret) = -1; });
        sp->set_call_back("foo_return_with_value1::pred",
                          [](void* pred) { *reinterpret_cast<bool*>(pred) = false; });

        sp->set_call_back("foo_return_with_void1::pred",
                          [](void* pred) { *reinterpret_cast<bool*>(pred) = false; });

        int ret = foo_return_with_value();
        assert(ret == -1);

        foo_return_with_void(&ret);
        assert(ret == 10086);
    }

    // test pred == true, get the value and return point we want
    {
        sp->clear_all_call_backs();
        sp->set_call_back("foo_return_with_value2",
                          [](void* ret) { *reinterpret_cast<int*>(ret) = 10086; });
        sp->set_call_back("foo_return_with_value2::pred",
                          [](void* pred) { *reinterpret_cast<bool*>(pred) = true; });

        sp->set_call_back("foo_return_with_void2::pred",
                          [](void* pred) { *reinterpret_cast<bool*>(pred) = true; });

        int ret = foo_return_with_value();
        assert(ret == 10086);

        foo_return_with_void(&ret);
        assert(ret == 10010);
    }

    // pred depends on tested thread's context
    {
        sp->clear_all_call_backs();
        int* ctx;
        // "steal" context from tested thread to this testing thread in order to
        // change behaviors of the tested thread without changing it's code
        sp->set_call_back("foo_return_with_value1_ctx",
                          [&ctx](void* tested_ctx) { ctx = reinterpret_cast<int*>(tested_ctx); });
        sp->set_call_back("foo_return_with_value1",
                          [](void* ret) { *reinterpret_cast<int*>(ret) = 10086; });
        // use the context from tested thread to do more checks and modifications
        sp->set_call_back("foo_return_with_value1::pred", [&ctx](void* pred) {
            // we can change the return logic of the tested thread
            *reinterpret_cast<bool*>(pred) = (*ctx > 0); // true
        });

        [[maybe_unused]] int ret = foo_return_with_value();
        assert(ret == 10086);
    }
}

void test() {
    //   test_foo_single_thread();
    //   test_foo_data_race();
    test_return_point();
}

int main() {
    test();
    return 0;
}
