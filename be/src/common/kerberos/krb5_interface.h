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

#include <krb5.h>

#include <memory>
#include <string>

#include "common/status.h"

namespace doris::kerberos {

// Interface for krb5 operations, can be mocked for testing
class Krb5Interface {
public:
    virtual ~Krb5Interface() = default;

    virtual Status init_context(krb5_context* context) = 0;
    virtual Status parse_name(krb5_context context, const char* name,
                              krb5_principal* principal) = 0;
    virtual Status kt_resolve(krb5_context context, const char* name, krb5_keytab* keytab) = 0;
    virtual Status cc_resolve(krb5_context context, const char* name, krb5_ccache* ccache) = 0;
    virtual Status get_init_creds_opt_alloc(krb5_context context,
                                            krb5_get_init_creds_opt** opt) = 0;
    virtual Status get_init_creds_keytab(krb5_context context, krb5_creds* creds,
                                         krb5_principal client, krb5_keytab keytab,
                                         krb5_deltat start, const char* in_tkt_service,
                                         krb5_get_init_creds_opt* options) = 0;
    virtual Status cc_initialize(krb5_context context, krb5_ccache cache,
                                 krb5_principal principal) = 0;
    virtual Status cc_store_cred(krb5_context context, krb5_ccache cache, krb5_creds* creds) = 0;
    virtual Status timeofday(krb5_context context, krb5_timestamp* timeret) = 0;
    virtual Status cc_start_seq_get(krb5_context context, krb5_ccache cache,
                                    krb5_cc_cursor* cursor) = 0;
    virtual Status cc_next_cred(krb5_context context, krb5_ccache cache, krb5_cc_cursor* cursor,
                                krb5_creds* creds) = 0;

    virtual void cc_end_seq_get(krb5_context context, krb5_ccache cache,
                                krb5_cc_cursor* cursor) = 0;
    virtual void free_principal(krb5_context context, krb5_principal principal) = 0;
    virtual void free_cred_contents(krb5_context context, krb5_creds* creds) = 0;
    virtual void get_init_creds_opt_free(krb5_context context, krb5_get_init_creds_opt* opt) = 0;
    virtual void kt_close(krb5_context context, krb5_keytab keytab) = 0;
    virtual void cc_close(krb5_context context, krb5_ccache cache) = 0;
    virtual void free_context(krb5_context context) = 0;
    virtual const char* get_error_message(krb5_context context, krb5_error_code code) = 0;
    virtual void free_error_message(krb5_context context, const char* message) = 0;
    virtual Status unparse_name(krb5_context context, krb5_principal principal, char** name) = 0;
    virtual void free_unparsed_name(krb5_context context, char* name) = 0;
};

// Factory to create Krb5Interface instances
class Krb5InterfaceFactory {
public:
    static std::unique_ptr<Krb5Interface> create();
};

} // namespace doris::kerberos
