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

#include "common/resource_tls.h"

#include <pthread.h>

#include "common/logging.h"
#include "gen_cpp/Types_types.h"

namespace doris {

static pthread_key_t s_resource_key;
static bool s_is_init = false;

static void resource_destructor(void* value) {
    TResourceInfo* info = (TResourceInfo*)value;
    if (info != nullptr) {
        delete info;
    }
}

void ResourceTls::init() {
    int ret = pthread_key_create(&s_resource_key, resource_destructor);
    if (ret != 0) {
        LOG(ERROR) << "create pthread key for resource failed.";
        return;
    }
    s_is_init = true;
}

TResourceInfo* ResourceTls::get_resource_tls() {
    if (!s_is_init) {
        return nullptr;
    }
    return (TResourceInfo*)pthread_getspecific(s_resource_key);
}

int ResourceTls::set_resource_tls(TResourceInfo* info) {
    if (!s_is_init) {
        return -1;
    }
    TResourceInfo* old_info = (TResourceInfo*)pthread_getspecific(s_resource_key);

    int ret = pthread_setspecific(s_resource_key, info);
    if (ret == 0) {
        // OK, now we delete old one
        delete old_info;
    }
    return ret;
}

} // namespace doris
