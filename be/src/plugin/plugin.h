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

namespace doris {

#define DORIS_PLUGIN_VERSION 0x0001

typedef struct st_plugin {
    void* handler;

    int (*init)(void *);

    int (*close)(void *);

    uint64_t flags;

    void* variable;

    void* status;
} Plugin;


#define declare_plugin(NAME)                                \
  __DECLARE_PLUGIN(NAME, ##NAME##_plugin_interface_version, \
                         ##NAME##_sizeof_struct_st_plugin,  \
                         ##NAME##_plugin)

#define __DECLARE_PLUGIN(NAME, VERSION, PSIZE, DECLS)   \
  int VERSION = DORIS_PLUGIN_VERSION;                   \
  int PSIZE = sizeof(struct st_plugin);                 \
  struct st_plugin DECLS[] = {

          
#define declare_plugin_end           \
  , { 0, 0, 0, 0, 0, 0 }             \
  }

}
