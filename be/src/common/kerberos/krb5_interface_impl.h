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

#include "common/kerberos/krb5_interface.h"

namespace doris::kerberos {

class Krb5InterfaceImpl : public Krb5Interface {
public:
    Status init_context(krb5_context* context) override;
    Status parse_name(krb5_context context, const char* name, krb5_principal* principal) override;
    Status kt_resolve(krb5_context context, const char* name, krb5_keytab* keytab) override;
    Status cc_resolve(krb5_context context, const char* name, krb5_ccache* ccache) override;
    Status get_init_creds_opt_alloc(krb5_context context, krb5_get_init_creds_opt** opt) override;
    Status get_init_creds_keytab(krb5_context context, krb5_creds* creds, krb5_principal client,
                                 krb5_keytab keytab, krb5_deltat start, const char* in_tkt_service,
                                 krb5_get_init_creds_opt* options) override;
    Status cc_initialize(krb5_context context, krb5_ccache cache,
                         krb5_principal principal) override;
    Status cc_store_cred(krb5_context context, krb5_ccache cache, krb5_creds* creds) override;
    Status timeofday(krb5_context context, krb5_timestamp* timeret) override;
    Status cc_start_seq_get(krb5_context context, krb5_ccache cache,
                            krb5_cc_cursor* cursor) override;
    Status cc_next_cred(krb5_context context, krb5_ccache cache, krb5_cc_cursor* cursor,
                        krb5_creds* creds) override;

    void cc_end_seq_get(krb5_context context, krb5_ccache cache, krb5_cc_cursor* cursor) override;
    void free_principal(krb5_context context, krb5_principal principal) override;
    void free_cred_contents(krb5_context context, krb5_creds* creds) override;
    void get_init_creds_opt_free(krb5_context context, krb5_get_init_creds_opt* opt) override;
    void kt_close(krb5_context context, krb5_keytab keytab) override;
    void cc_close(krb5_context context, krb5_ccache cache) override;
    void free_context(krb5_context context) override;
    const char* get_error_message(krb5_context context, krb5_error_code code) override;
    void free_error_message(krb5_context context, const char* message) override;
    Status unparse_name(krb5_context context, krb5_principal principal, char** name) override;
    void free_unparsed_name(krb5_context context, char* name) override;

private:
    Status _check_error(krb5_error_code code, krb5_context context, const char* message);
};

} // namespace doris::kerberos
