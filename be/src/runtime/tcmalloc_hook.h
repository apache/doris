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

#include <gperftools/malloc_hook.h>
#include <gperftools/nallocx.h>
#include <gperftools/tcmalloc.h>

#include <cassert>

#include "runtime/thread_status.h"

static int new_hook_calls = 0;
static int delete_hook_calls = 0;

void new_hook(const void* ptr, size_t size) {
    new_hook_calls++;
    doris::current_thread.consume_mem(tc_nallocx(size, 0));
}

void delete_hook(const void* ptr) {
    delete_hook_calls++;
    doris::current_thread.release_mem(tc_malloc_size(const_cast<void*>(ptr)));
}

void init_hook() {
    assert(MallocHook::AddNewHook(&new_hook));
    assert(MallocHook::AddDeleteHook(&delete_hook));
}

void destroy_hook() {
    assert(MallocHook::RemoveNewHook(&new_hook));
    assert(MallocHook::RemoveDeleteHook(&delete_hook));
}