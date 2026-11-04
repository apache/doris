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

suite("test_lead_lag_large_offset") {
    sql "SET enable_nereids_planner = true"
    sql "SET enable_fallback_to_original_planner = false"

    /*
    Note: offset value is not likely to be such big as 9223372036854775807,
    so currently BE does not check for int64 overflow, and BE will crash:
    ```
../src/exec/operator/analytic_sink_operator.cpp:229:76: runtime error: signed integer overflow: 9223372036854775807 + 1 cannot be represented in type 'int64_t' (aka 'long')
    #0 0x55efa53c8f4f in doris::AnalyticSinkLocalState::_get_next_for_unbounded_rows(long, long) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:229:76
    #1 0x55efa53d827a in doris::AnalyticSinkLocalState::_execute_impl(doris::RuntimeState*) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:358:33
    #2 0x55efa53e9e33 in doris::AnalyticSinkOperatorX::sink_impl(doris::RuntimeState*, doris::Block*, bool) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:754:5
    #3 0x55efa0a2b660 in doris::DataSinkOperatorXBase::sink(doris::RuntimeState*, doris::Block*, bool) be/build_ASAN/../src/exec/operator/operator.h:621:16
    #4 0x55efa09f2649 in doris::PipelineTask::execute(bool*) be/build_ASAN/../src/exec/pipeline/pipeline_task.cpp:726:29
    #5 0x55efa85d9abf in doris::TaskScheduler::_do_work(int) be/build_ASAN/../src/exec/pipeline/task_scheduler.cpp:151:13
    #6 0x55efa85ddf3c in doris::TaskScheduler::start()::$_0::operator()() const be/build_ASAN/../src/exec/pipeline/task_scheduler.cpp:64:9
    #7 0x55efa85dde2c in void std::__invoke_impl<void, doris::TaskScheduler::start()::$_0&>(std::__invoke_other, doris::TaskScheduler::start()::$_0&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:63:14
    #8 0x55efa85ddd4c in std::enable_if<is_invocable_r_v<void, doris::TaskScheduler::start()::$_0&>, void>::type std::__invoke_r<void, doris::TaskScheduler::start()::$_0&>(doris::TaskScheduler::start()::$_0&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:113:2
    #9 0x55efa85dda24 in std::_Function_handler<void (), doris::TaskScheduler::start()::$_0>::_M_invoke(std::_Any_data const&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:292:9
    #10 0x55ef7d4bf9ad in std::function<void ()>::operator()() const /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:593:9
    #11 0x55efa9f150b0 in doris::FunctionRunnable::run() be/build_ASAN/../src/util/threadpool.cpp:60:27
    #12 0x55efa9ef6ced in doris::ThreadPool::dispatch_thread() be/build_ASAN/../src/util/threadpool.cpp:621:24
    #13 0x55efa9f356fc in void std::__invoke_impl<void, void (doris::ThreadPool::*&)(), doris::ThreadPool*&>(std::__invoke_memfun_deref, void (doris::ThreadPool::*&)(), doris::ThreadPool*&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:76:14
    #14 0x55efa9f354b4 in std::__invoke_result<void (doris::ThreadPool::*&)(), doris::ThreadPool*&>::type std::__invoke<void (doris::ThreadPool::*&)(), doris::ThreadPool*&>(void (doris::ThreadPool::*&)(), doris::ThreadPool*&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:98:14
    #15 0x55efa9f353e0 in void std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>::__call<void, 0ul>(std::tuple<>&&, std::_Index_tuple<0ul>) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/functional:515:11
    #16 0x55efa9f3519b in void std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>::operator()<void>() /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/functional:600:17
    #17 0x55efa9f3508c in void std::__invoke_impl<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>(std::__invoke_other, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:63:14
    #18 0x55efa9f34f8c in std::enable_if<is_invocable_r_v<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>, void>::type std::__invoke_r<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>(std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:113:2
    #19 0x55efa9f34864 in std::_Function_handler<void (), std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>>::_M_invoke(std::_Any_data const&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:292:9
    #20 0x55ef7d4bf9ad in std::function<void ()>::operator()() const /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:593:9
    #21 0x55efa9eafd98 in doris::Thread::supervise_thread(void*) be/build_ASAN/../src/util/thread.cpp:460:5
    #22 0x55ef7d205d26 in asan_thread_start(void*) (/mnt/disk2/tengjianping/wt-master/wt-memleak3/output/be/lib/doris_be+0x4b80dd26)
    #23 0x7f96c0c8b698 in start_thread (/lib64/libc.so.6+0x8b698) (BuildId: 65d7e434cec6326711148d1465614ba5c96649c1)
    #24 0x7f96c0d1089f in __GI___clone3 (/lib64/libc.so.6+0x11089f) (BuildId: 65d7e434cec6326711148d1465614ba5c96649c1)

SUMMARY: UndefinedBehaviorSanitizer: undefined-behavior ../src/exec/operator/analytic_sink_operator.cpp:229:76
../src/exec/operator/analytic_sink_operator.cpp:240:77: runtime error: signed integer overflow: -9223372036854775808 - 1 cannot be represented in type 'int64_t' (aka 'long')
    #0 0x55efa53c96e5 in doris::AnalyticSinkLocalState::_get_next_for_unbounded_rows(long, long) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:240:77
    #1 0x55efa53d827a in doris::AnalyticSinkLocalState::_execute_impl(doris::RuntimeState*) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:358:33
    #2 0x55efa53e9e33 in doris::AnalyticSinkOperatorX::sink_impl(doris::RuntimeState*, doris::Block*, bool) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:754:5
    #3 0x55efa0a2b660 in doris::DataSinkOperatorXBase::sink(doris::RuntimeState*, doris::Block*, bool) be/build_ASAN/../src/exec/operator/operator.h:621:16
    #4 0x55efa09f2649 in doris::PipelineTask::execute(bool*) be/build_ASAN/../src/exec/pipeline/pipeline_task.cpp:726:29
    #5 0x55efa85d9abf in doris::TaskScheduler::_do_work(int) be/build_ASAN/../src/exec/pipeline/task_scheduler.cpp:151:13
    #6 0x55efa85ddf3c in doris::TaskScheduler::start()::$_0::operator()() const be/build_ASAN/../src/exec/pipeline/task_scheduler.cpp:64:9
    #7 0x55efa85dde2c in void std::__invoke_impl<void, doris::TaskScheduler::start()::$_0&>(std::__invoke_other, doris::TaskScheduler::start()::$_0&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:63:14
    #8 0x55efa85ddd4c in std::enable_if<is_invocable_r_v<void, doris::TaskScheduler::start()::$_0&>, void>::type std::__invoke_r<void, doris::TaskScheduler::start()::$_0&>(doris::TaskScheduler::start()::$_0&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:113:2
    #9 0x55efa85dda24 in std::_Function_handler<void (), doris::TaskScheduler::start()::$_0>::_M_invoke(std::_Any_data const&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:292:9
    #10 0x55ef7d4bf9ad in std::function<void ()>::operator()() const /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:593:9
    #11 0x55efa9f150b0 in doris::FunctionRunnable::run() be/build_ASAN/../src/util/threadpool.cpp:60:27
    #12 0x55efa9ef6ced in doris::ThreadPool::dispatch_thread() be/build_ASAN/../src/util/threadpool.cpp:621:24
    #13 0x55efa9f356fc in void std::__invoke_impl<void, void (doris::ThreadPool::*&)(), doris::ThreadPool*&>(std::__invoke_memfun_deref, void (doris::ThreadPool::*&)(), doris::ThreadPool*&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:76:14
    #14 0x55efa9f354b4 in std::__invoke_result<void (doris::ThreadPool::*&)(), doris::ThreadPool*&>::type std::__invoke<void (doris::ThreadPool::*&)(), doris::ThreadPool*&>(void (doris::ThreadPool::*&)(), doris::ThreadPool*&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:98:14
    #15 0x55efa9f353e0 in void std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>::__call<void, 0ul>(std::tuple<>&&, std::_Index_tuple<0ul>) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/functional:515:11
    #16 0x55efa9f3519b in void std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>::operator()<void>() /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/functional:600:17
    #17 0x55efa9f3508c in void std::__invoke_impl<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>(std::__invoke_other, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:63:14
    #18 0x55efa9f34f8c in std::enable_if<is_invocable_r_v<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>, void>::type std::__invoke_r<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>(std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:113:2
    #19 0x55efa9f34864 in std::_Function_handler<void (), std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>>::_M_invoke(std::_Any_data const&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:292:9
    #20 0x55ef7d4bf9ad in std::function<void ()>::operator()() const /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:593:9
    #21 0x55efa9eafd98 in doris::Thread::supervise_thread(void*) be/build_ASAN/../src/util/thread.cpp:460:5
    #22 0x55ef7d205d26 in asan_thread_start(void*) (/mnt/disk2/tengjianping/wt-master/wt-memleak3/output/be/lib/doris_be+0x4b80dd26)
    #23 0x7f96c0c8b698 in start_thread (/lib64/libc.so.6+0x8b698) (BuildId: 65d7e434cec6326711148d1465614ba5c96649c1)
    #24 0x7f96c0d1089f in __GI___clone3 (/lib64/libc.so.6+0x11089f) (BuildId: 65d7e434cec6326711148d1465614ba5c96649c1)

SUMMARY: UndefinedBehaviorSanitizer: undefined-behavior ../src/exec/operator/analytic_sink_operator.cpp:240:77
../src/exec/operator/analytic_sink_operator.cpp:242:97: runtime error: signed integer overflow: -9223372036854775808 - 1 cannot be represented in type 'int64_t' (aka 'long')
    #0 0x55efa53c993a in doris::AnalyticSinkLocalState::_get_next_for_unbounded_rows(long, long) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:242:97
    #1 0x55efa53d827a in doris::AnalyticSinkLocalState::_execute_impl(doris::RuntimeState*) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:358:33
    #2 0x55efa53e9e33 in doris::AnalyticSinkOperatorX::sink_impl(doris::RuntimeState*, doris::Block*, bool) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:754:5
    #3 0x55efa0a2b660 in doris::DataSinkOperatorXBase::sink(doris::RuntimeState*, doris::Block*, bool) be/build_ASAN/../src/exec/operator/operator.h:621:16
    #4 0x55efa09f2649 in doris::PipelineTask::execute(bool*) be/build_ASAN/../src/exec/pipeline/pipeline_task.cpp:726:29
    #5 0x55efa85d9abf in doris::TaskScheduler::_do_work(int) be/build_ASAN/../src/exec/pipeline/task_scheduler.cpp:151:13
    #6 0x55efa85ddf3c in doris::TaskScheduler::start()::$_0::operator()() const be/build_ASAN/../src/exec/pipeline/task_scheduler.cpp:64:9
    #7 0x55efa85dde2c in void std::__invoke_impl<void, doris::TaskScheduler::start()::$_0&>(std::__invoke_other, doris::TaskScheduler::start()::$_0&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:63:14
    #8 0x55efa85ddd4c in std::enable_if<is_invocable_r_v<void, doris::TaskScheduler::start()::$_0&>, void>::type std::__invoke_r<void, doris::TaskScheduler::start()::$_0&>(doris::TaskScheduler::start()::$_0&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:113:2
    #9 0x55efa85dda24 in std::_Function_handler<void (), doris::TaskScheduler::start()::$_0>::_M_invoke(std::_Any_data const&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:292:9
    #10 0x55ef7d4bf9ad in std::function<void ()>::operator()() const /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:593:9
    #11 0x55efa9f150b0 in doris::FunctionRunnable::run() be/build_ASAN/../src/util/threadpool.cpp:60:27
    #12 0x55efa9ef6ced in doris::ThreadPool::dispatch_thread() be/build_ASAN/../src/util/threadpool.cpp:621:24
    #13 0x55efa9f356fc in void std::__invoke_impl<void, void (doris::ThreadPool::*&)(), doris::ThreadPool*&>(std::__invoke_memfun_deref, void (doris::ThreadPool::*&)(), doris::ThreadPool*&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:76:14
    #14 0x55efa9f354b4 in std::__invoke_result<void (doris::ThreadPool::*&)(), doris::ThreadPool*&>::type std::__invoke<void (doris::ThreadPool::*&)(), doris::ThreadPool*&>(void (doris::ThreadPool::*&)(), doris::ThreadPool*&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:98:14
    #15 0x55efa9f353e0 in void std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>::__call<void, 0ul>(std::tuple<>&&, std::_Index_tuple<0ul>) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/functional:515:11
    #16 0x55efa9f3519b in void std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>::operator()<void>() /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/functional:600:17
    #17 0x55efa9f3508c in void std::__invoke_impl<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>(std::__invoke_other, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:63:14
    #18 0x55efa9f34f8c in std::enable_if<is_invocable_r_v<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>, void>::type std::__invoke_r<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>(std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:113:2
    #19 0x55efa9f34864 in std::_Function_handler<void (), std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>>::_M_invoke(std::_Any_data const&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:292:9
    #20 0x55ef7d4bf9ad in std::function<void ()>::operator()() const /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:593:9
    #21 0x55efa9eafd98 in doris::Thread::supervise_thread(void*) be/build_ASAN/../src/util/thread.cpp:460:5
    #22 0x55ef7d205d26 in asan_thread_start(void*) (/mnt/disk2/tengjianping/wt-master/wt-memleak3/output/be/lib/doris_be+0x4b80dd26)
    #23 0x7f96c0c8b698 in start_thread (/lib64/libc.so.6+0x8b698) (BuildId: 65d7e434cec6326711148d1465614ba5c96649c1)
    #24 0x7f96c0d1089f in __GI___clone3 (/lib64/libc.so.6+0x11089f) (BuildId: 65d7e434cec6326711148d1465614ba5c96649c1)

SUMMARY: UndefinedBehaviorSanitizer: undefined-behavior ../src/exec/operator/analytic_sink_operator.cpp:242:97
../src/exprs/aggregate/aggregate_function_window.h:512:44: runtime error: signed integer overflow: -9223372036854775808 - 1 cannot be represented in type 'int64_t' (aka 'long')
    #0 0x55ef89702528 in doris::WindowFunctionLeadImpl<doris::LeadLagData<true, false>, false>::add_range_single_place(long, long, long, long, doris::IColumn const**) be/build_ASAN/../src/exprs/aggregate/aggregate_function_window.h:512:44
    #1 0x55ef896f9914 in doris::WindowFunctionData<doris::WindowFunctionLeadImpl<doris::LeadLagData<true, false>, false>>::add_range_single_place(long, long, long, long, char*, doris::IColumn const**, doris::Arena&, unsigned char*, unsigned char*) const be/build_ASAN/../src/exprs/aggregate/aggregate_function_window.h:674:27
    #2 0x55efa542f314 in doris::AggFnEvaluator::add_range_single_place(long, long, long, long, char*, doris::IColumn const**, doris::Arena&, unsigned char*, unsigned char*) be/build_ASAN/../src/exprs/vectorized_agg_fn.cpp:325:16
    #3 0x55efa5402ce6 in void doris::AnalyticSinkLocalState::_execute_for_function<false>(long, long, long, long) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:391:32
    #4 0x55efa53c995c in doris::AnalyticSinkLocalState::_get_next_for_unbounded_rows(long, long) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:242:9
    #5 0x55efa53d827a in doris::AnalyticSinkLocalState::_execute_impl(doris::RuntimeState*) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:358:33
    #6 0x55efa53e9e33 in doris::AnalyticSinkOperatorX::sink_impl(doris::RuntimeState*, doris::Block*, bool) be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:754:5
    #7 0x55efa0a2b660 in doris::DataSinkOperatorXBase::sink(doris::RuntimeState*, doris::Block*, bool) be/build_ASAN/../src/exec/operator/operator.h:621:16
    #8 0x55efa09f2649 in doris::PipelineTask::execute(bool*) be/build_ASAN/../src/exec/pipeline/pipeline_task.cpp:726:29
    #9 0x55efa85d9abf in doris::TaskScheduler::_do_work(int) be/build_ASAN/../src/exec/pipeline/task_scheduler.cpp:151:13
    #10 0x55efa85ddf3c in doris::TaskScheduler::start()::$_0::operator()() const be/build_ASAN/../src/exec/pipeline/task_scheduler.cpp:64:9
    #11 0x55efa85dde2c in void std::__invoke_impl<void, doris::TaskScheduler::start()::$_0&>(std::__invoke_other, doris::TaskScheduler::start()::$_0&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:63:14
    #12 0x55efa85ddd4c in std::enable_if<is_invocable_r_v<void, doris::TaskScheduler::start()::$_0&>, void>::type std::__invoke_r<void, doris::TaskScheduler::start()::$_0&>(doris::TaskScheduler::start()::$_0&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:113:2
    #13 0x55efa85dda24 in std::_Function_handler<void (), doris::TaskScheduler::start()::$_0>::_M_invoke(std::_Any_data const&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:292:9
    #14 0x55ef7d4bf9ad in std::function<void ()>::operator()() const /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:593:9
    #15 0x55efa9f150b0 in doris::FunctionRunnable::run() be/build_ASAN/../src/util/threadpool.cpp:60:27
    #16 0x55efa9ef6ced in doris::ThreadPool::dispatch_thread() be/build_ASAN/../src/util/threadpool.cpp:621:24
    #17 0x55efa9f356fc in void std::__invoke_impl<void, void (doris::ThreadPool::*&)(), doris::ThreadPool*&>(std::__invoke_memfun_deref, void (doris::ThreadPool::*&)(), doris::ThreadPool*&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:76:14
    #18 0x55efa9f354b4 in std::__invoke_result<void (doris::ThreadPool::*&)(), doris::ThreadPool*&>::type std::__invoke<void (doris::ThreadPool::*&)(), doris::ThreadPool*&>(void (doris::ThreadPool::*&)(), doris::ThreadPool*&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:98:14
    #19 0x55efa9f353e0 in void std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>::__call<void, 0ul>(std::tuple<>&&, std::_Index_tuple<0ul>) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/functional:515:11
    #20 0x55efa9f3519b in void std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>::operator()<void>() /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/functional:600:17
    #21 0x55efa9f3508c in void std::__invoke_impl<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>(std::__invoke_other, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:63:14
    #22 0x55efa9f34f8c in std::enable_if<is_invocable_r_v<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>, void>::type std::__invoke_r<void, std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&>(std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/invoke.h:113:2
    #23 0x55efa9f34864 in std::_Function_handler<void (), std::_Bind<void (doris::ThreadPool::* (doris::ThreadPool*))()>>::_M_invoke(std::_Any_data const&) /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:292:9
    #24 0x55ef7d4bf9ad in std::function<void ()>::operator()() const /mnt/disk2/tengjianping/local/ldb_toolchain/bin/../lib/gcc/x86_64-pc-linux-gnu/15/include/g++-v15/bits/std_function.h:593:9
    #25 0x55efa9eafd98 in doris::Thread::supervise_thread(void*) be/build_ASAN/../src/util/thread.cpp:460:5
    #26 0x55ef7d205d26 in asan_thread_start(void*) (/mnt/disk2/tengjianping/wt-master/wt-memleak3/output/be/lib/doris_be+0x4b80dd26)
    #27 0x7f96c0c8b698 in start_thread (/lib64/libc.so.6+0x8b698) (BuildId: 65d7e434cec6326711148d1465614ba5c96649c1)
    #28 0x7f96c0d1089f in __GI___clone3 (/lib64/libc.so.6+0x11089f) (BuildId: 65d7e434cec6326711148d1465614ba5c96649c1)

SUMMARY: UndefinedBehaviorSanitizer: undefined-behavior ../src/exprs/aggregate/aggregate_function_window.h:512:44
F 2026-07-27 11:47:15,677 252267 pod_array.h:370] Check failed: n <= static_cast<ssize_t>(this->size()) (9223372036854775807 vs. 2)
*** Check failure stack trace: ***
    @     0x55efaefc1366  google::LogMessageFatal::~LogMessageFatal()
    @     0x55ef7ec42fe8  doris::PODArray<>::operator[]()
    @     0x55ef7ed21415  doris::ColumnVector<>::get_data_at()
    @     0x55ef896d7ada  doris::BaseValue<>::get_value()
    @     0x55ef897010b3  doris::LeadLagData<>::insert_result_into()
    @     0x55ef896f86d4  doris::WindowFunctionData<>::insert_result_into()
    @     0x55ef815c7d46  doris::IAggregateFunction::insert_result_into_range()
    @     0x55efa5430265  doris::AggFnEvaluator::insert_result_info_range()
    @     0x55efa53d4467  doris::AnalyticSinkLocalState::_insert_result_info()
    @     0x55efa53c9a81  doris::AnalyticSinkLocalState::_get_next_for_unbounded_rows()
    @     0x55efa53d827b  doris::AnalyticSinkLocalState::_execute_impl()
    @     0x55efa53e9e34  doris::AnalyticSinkOperatorX::sink_impl()
    @     0x55efa0a2b661  doris::DataSinkOperatorXBase::sink()
    @     0x55efa09f264a  doris::PipelineTask::execute()
    @     0x55efa85d9ac0  doris::TaskScheduler::_do_work()
    @     0x55efa85ddf3d  doris::TaskScheduler::start()::$_0::operator()()
    @     0x55efa85dde2d  std::__invoke_impl<>()
    @     0x55efa85ddd4d  _ZSt10__invoke_rIvRZN5doris13TaskScheduler5startEvE3$_0JEENSt9enable_ifIX16is_invocable_r_vIT_T0_DpT1_EES5_E4typeEOS6_DpOS7_
    @     0x55efa85dda25  std::_Function_handler<>::_M_invoke()
    @     0x55ef7d4bf9ae  std::function<>::operator()()
    @     0x55efa9f150b1  doris::FunctionRunnable::run()
    @     0x55efa9ef6cee  doris::ThreadPool::dispatch_thread()
    @     0x55efa9f356fd  std::__invoke_impl<>()
    @     0x55efa9f354b5  std::__invoke<>()
    @     0x55efa9f353e1  _ZNSt5_BindIFMN5doris10ThreadPoolEFvvEPS1_EE6__callIvJEJLm0EEEET_OSt5tupleIJDpT0_EESt12_Index_tupleIJXspT1_EEE
    @     0x55efa9f3519c  std::_Bind<>::operator()<>()
    @     0x55efa9f3508d  std::__invoke_impl<>()
    @     0x55efa9f34f8d  _ZSt10__invoke_rIvRSt5_BindIFMN5doris10ThreadPoolEFvvEPS2_EEJEENSt9enable_ifIX16is_invocable_r_vIT_T0_DpT1_EESA_E4typeEOSB_DpOSC_
    @     0x55efa9f34865  std::_Function_handler<>::_M_invoke()
    @     0x55ef7d4bf9ae  std::function<>::operator()()
    @     0x55efa9eafd99  doris::Thread::supervise_thread()
    @     0x55ef7d205d27  asan_thread_start()
*** Query id: 8689a709882f431d-9c24d9d0d8a35473 ***
*** tablet id: 0 ***
*** Aborted at 1785124036 (unix time) try "date -d @1785124036" if you are using GNU date ***
*** Current BE git commitID: 5e673d5a4a3 ***
*** SIGABRT unknown detail explain (@0x3eb0003c20c) received by PID 246284 (TID 252267 OR 0x77fbc810d640) from PID 246284; stack trace: ***
 0# doris::signal::(anonymous namespace)::FailureSignalHandler(int, siginfo_t*, void*) at ../src/common/signal_handler.h:417
 1# 0x00007F96C0C3FC60 in /lib64/libc.so.6
 2# __pthread_kill_implementation in /lib64/libc.so.6
 3# gsignal in /lib64/libc.so.6
 4# abort in /lib64/libc.so.6
 5# 0x000055EFAEFC8CED in /mnt/disk2/tengjianping/wt-master/wt-memleak3/output/be/lib/doris_be
 6# google::LogMessage::SendToLog() in /mnt/disk2/tengjianping/wt-master/wt-memleak3/output/be/lib/doris_be
 7# google::LogMessage::Flush() in /mnt/disk2/tengjianping/wt-master/wt-memleak3/output/be/lib/doris_be
 8# google::LogMessageFatal::~LogMessageFatal() in /mnt/disk2/tengjianping/wt-master/wt-memleak3/output/be/lib/doris_be
 9# doris::PODArray<signed char, 4096ul, doris::Allocator<false, false, false, doris::DefaultMemoryAllocator, true>, 16ul, 15ul>::operator[](long) const at ../src/core/pod_array.h:370
10# doris::ColumnVector<(doris::PrimitiveType)3>::get_data_at(unsigned long) const at ../src/core/column/column_vector.h:102
11# doris::BaseValue<false>::get_value() const at ../src/exprs/aggregate/aggregate_function_window.h:426
12# doris::LeadLagData<true, false>::insert_result_into(doris::IColumn&) const at ../src/exprs/aggregate/aggregate_function_window.h:446
13# doris::WindowFunctionData<doris::WindowFunctionLeadImpl<doris::LeadLagData<true, false>, false> >::insert_result_into(char const*, doris::IColumn&) const at ../src/exprs/aggregate/aggregate_function_window.h:682
14# doris::IAggregateFunction::insert_result_into_range(char const*, doris::IColumn&, unsigned long, unsigned long) const at ../src/exprs/aggregate/aggregate_function.h:270
15# doris::AggFnEvaluator::insert_result_info_range(char const*, doris::IColumn*, unsigned long, unsigned long) at ./be/build_ASAN/../src/exprs/vectorized_agg_fn.cpp:354
16# doris::AnalyticSinkLocalState::_insert_result_info(long, long) at ./be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:419
17# doris::AnalyticSinkLocalState::_get_next_for_unbounded_rows(long, long) at ./be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:245
18# doris::AnalyticSinkLocalState::_execute_impl(doris::RuntimeState*) at ./be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:358
19# doris::AnalyticSinkOperatorX::sink_impl(doris::RuntimeState*, doris::Block*, bool) at ./be/build_ASAN/../src/exec/operator/analytic_sink_operator.cpp:754
20# doris::DataSinkOperatorXBase::sink(doris::RuntimeState*, doris::Block*, bool) in /mnt/disk2/tengjianping/wt-master/wt-memleak3/output/be/lib/doris_be
21# doris::PipelineTask::execute(bool*) at ./be/build_ASAN/../src/exec/pipeline/pipeline_task.cpp:726
22# doris::TaskScheduler::_do_work(int) at ./be/build_ASAN/../src/exec/pipeline/task_scheduler.cpp:151
    ```
    // RQG test cases
    order_qt_lead_max_int64 """
        SELECT k, LEAD(k, 9223372036854775807) OVER (ORDER BY k) AS lag_big
        FROM (SELECT 1 AS k UNION ALL SELECT 2) t
    """

    order_qt_lag_max_int64 """
        SELECT k, LAG(k, 9223372036854775807) OVER (ORDER BY k) AS lag_big
        FROM (SELECT 1 AS k UNION ALL SELECT 2) t
    """

    order_qt_lag_max_int64_with_default """
        SELECT k, LAG(k, 9223372036854775807, k * 10) OVER (ORDER BY k) AS lag_big
        FROM (SELECT 1 AS k UNION ALL SELECT 2) t
    """

    multi_sql """
    DROP TABLE IF EXISTS tmp_window_offset_extreme_probe;
    CREATE TABLE tmp_window_offset_extreme_probe (
        k INT
    )
    DUPLICATE KEY(k)
    DISTRIBUTED BY HASH(k) BUCKETS 1
    PROPERTIES('replication_num'='1');

    INSERT INTO tmp_window_offset_extreme_probe VALUES (1), (2), (3);
    """
    order_qt_lead_big_int64 """
        SELECT k,
               LEAD(k, 9223372036854775800) OVER(ORDER BY k) AS lead_near
        FROM tmp_window_offset_extreme_probe
    """
    */

    test {
        sql """
            SELECT k, SUM(k) OVER (
                ORDER BY k ROWS BETWEEN 9223372036854775808 PRECEDING AND CURRENT ROW
            ) AS sum_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "BoundOffset of ROWS WindowFrame must not exceed 9223372036854775807"
    }

    ["'abc'", "NULL", "TRUE", "DATE '2026-07-27'"].each { invalidOffset ->
        test {
            sql """
                SELECT k, SUM(k) OVER (
                    ORDER BY k ROWS BETWEEN ${invalidOffset} PRECEDING AND CURRENT ROW
                ) AS sum_invalid_offset
                FROM (SELECT 1 AS k UNION ALL SELECT 2) t
            """
            exception "BoundOffset of ROWS WindowFrame must be an Integer"
        }
    }

    test {
        sql """
            SELECT k, LAG(k, 9223372036854775808) OVER (ORDER BY k) AS lag_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "The offset parameter of LAG must not exceed 9223372036854775807"
    }

    test {
        sql """
            SELECT k, LEAD(k, 9223372036854775808) OVER (ORDER BY k) AS lead_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "The offset parameter of LEAD must not exceed 9223372036854775807"
    }

    test {
        sql """
            SELECT k, LAG(k, 922337203685477580.1) OVER (ORDER BY k) AS lag_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "The offset parameter of LAG must be a constant positive integer"
    }

    test {
        sql """
            SELECT k, LEAD(k, 922337203685477580.1) OVER (ORDER BY k) AS lead_big
            FROM (SELECT 1 AS k UNION ALL SELECT 2) t
        """
        exception "The offset parameter of LEAD must be a constant positive integer"
    }
}
