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

#include "runtime/process_profile.h"

#include <memory>

#include "olap/metadata_adder.h"
#include "runtime/memory/memory_profile.h"

namespace doris {

ProcessProfile::ProcessProfile() {
    _memory_profile = std::make_unique<MemoryProfile>();
}

void ProcessProfile::refresh_profile() {
    // 1. refresh profile
    _memory_profile->refresh_memory_overview_profile();
    _memory_profile->refresh_tasks_memory_profile();
    // TODO refresh other profile

    // 2. make profile
    std::unique_ptr<RuntimeProfile> process_profile =
            std::make_unique<RuntimeProfile>("ProcessProfile");
    _memory_profile->make_memory_profile(process_profile.get());
    // TODO make other profile

    // 3. dump object heap
    RuntimeProfile* object_heap_dump_snapshot =
            process_profile->create_child("ObjectHeapDump", true, false);
    MetadataAdder<ProcessProfile>::dump_metadata_object(object_heap_dump_snapshot);
    // TODO dump other object (block, column, etc.)

    _process_profile.set(std::move(process_profile));
}

} // namespace doris
