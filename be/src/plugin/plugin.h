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

#define PLUGIN_NOT_DYNAMIC_INSTALL 1UL
#define PLUGIN_NOT_DYNAMIC_UNINSTALL 2UL
#define PLUGIN_INSTALL_EARLY 4UL

#define DORIS_PLUGIN_VERSION 001100UL

/**
 * define a plugin:
 * 
 * declare_plugin(PLUGIN_NAME) {
 *     xx_handler,
 *     init_method,
 *     close_method,
 *     PLUGIN_NOT_DYNAMIC_INSTALL | PLUGIN_NOT_DYNAMIC_UNINSTALL,
 *     NULL,
 *     NULL
 * } declare_plugin_end
 * 
 */
struct Plugin {
    // support by type-specific plugin
    void* handler;

    // invoke when plugin install
    int (*init)(void *);

    // invoke when plugin uninstall
    int (*close)(void *);

    // flag for plugin 
    uint64_t flags;

    // use to set/get variables
    void* variable;

    // return the plugin's status
    void* status;
};


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
