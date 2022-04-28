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

// all header need by brpc is contain in this file.
// include this file instead of include <brpc/xxx.h>.

#include <service/brpc_conflict.h>

#include <brpc/channel.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/protocol.h>
#include <brpc/reloadable_flags.h>
#include <brpc/server.h>
#include <bthread/bthread.h>
#include <bthread/types.h>
#include <butil/containers/flat_map.h>
#include <butil/containers/flat_map_inl.h>
#include <butil/endpoint.h>
#include <butil/fd_utility.h>
#include <butil/macros.h>
