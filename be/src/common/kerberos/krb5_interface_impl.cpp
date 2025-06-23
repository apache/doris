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

#include "common/kerberos/krb5_interface_impl.h"

namespace doris::kerberos {

Status Krb5InterfaceImpl::init_context(krb5_context* context) {
    krb5_error_code code = krb5_init_context(context);
    if (code != 0) {
        return Status::InternalError("Failed to initialize krb5 context, error code: {}", code);
    }
    return Status::OK();
}

Status Krb5InterfaceImpl::parse_name(krb5_context context, const char* name,
                                     krb5_principal* principal) {
    krb5_error_code code = krb5_parse_name(context, name, principal);
    return _check_error(code, context, "Failed to parse principal name");
}

Status Krb5InterfaceImpl::kt_resolve(krb5_context context, const char* name, krb5_keytab* keytab) {
    krb5_error_code code = krb5_kt_resolve(context, name, keytab);
    return _check_error(code, context, "Failed to resolve keytab");
}

Status Krb5InterfaceImpl::cc_resolve(krb5_context context, const char* name, krb5_ccache* ccache) {
    krb5_error_code code = krb5_cc_resolve(context, name, ccache);
    return _check_error(code, context, "Failed to resolve credential cache");
}

Status Krb5InterfaceImpl::get_init_creds_opt_alloc(krb5_context context,
                                                   krb5_get_init_creds_opt** opt) {
    krb5_error_code code = krb5_get_init_creds_opt_alloc(context, opt);
    return _check_error(code, context, "Failed to allocate get_init_creds_opt");
}

Status Krb5InterfaceImpl::get_init_creds_keytab(krb5_context context, krb5_creds* creds,
                                                krb5_principal client, krb5_keytab keytab,
                                                krb5_deltat start, const char* in_tkt_service,
                                                krb5_get_init_creds_opt* options) {
    krb5_error_code code = krb5_get_init_creds_keytab(context, creds, client, keytab, start,
                                                      in_tkt_service, options);
    return _check_error(code, context, "Failed to get initial credentials");
}

Status Krb5InterfaceImpl::cc_initialize(krb5_context context, krb5_ccache cache,
                                        krb5_principal principal) {
    krb5_error_code code = krb5_cc_initialize(context, cache, principal);
    return _check_error(code, context, "Failed to initialize credential cache");
}

Status Krb5InterfaceImpl::cc_store_cred(krb5_context context, krb5_ccache cache,
                                        krb5_creds* creds) {
    krb5_error_code code = krb5_cc_store_cred(context, cache, creds);
    return _check_error(code, context, "Failed to store credentials");
}

Status Krb5InterfaceImpl::timeofday(krb5_context context, krb5_timestamp* timeret) {
    krb5_error_code code = krb5_timeofday(context, timeret);
    return _check_error(code, context, "Failed to get current time");
}

Status Krb5InterfaceImpl::cc_start_seq_get(krb5_context context, krb5_ccache cache,
                                           krb5_cc_cursor* cursor) {
    krb5_error_code code = krb5_cc_start_seq_get(context, cache, cursor);
    return _check_error(code, context, "Failed to start credential iteration");
}

Status Krb5InterfaceImpl::cc_next_cred(krb5_context context, krb5_ccache cache,
                                       krb5_cc_cursor* cursor, krb5_creds* creds) {
    krb5_error_code code = krb5_cc_next_cred(context, cache, cursor, creds);
    return _check_error(code, context, "Failed to get next credential");
}

void Krb5InterfaceImpl::cc_end_seq_get(krb5_context context, krb5_ccache cache,
                                       krb5_cc_cursor* cursor) {
    krb5_cc_end_seq_get(context, cache, cursor);
}

void Krb5InterfaceImpl::free_principal(krb5_context context, krb5_principal principal) {
    krb5_free_principal(context, principal);
}

void Krb5InterfaceImpl::free_cred_contents(krb5_context context, krb5_creds* creds) {
    krb5_free_cred_contents(context, creds);
}

void Krb5InterfaceImpl::get_init_creds_opt_free(krb5_context context,
                                                krb5_get_init_creds_opt* opt) {
    krb5_get_init_creds_opt_free(context, opt);
}

void Krb5InterfaceImpl::kt_close(krb5_context context, krb5_keytab keytab) {
    krb5_kt_close(context, keytab);
}

void Krb5InterfaceImpl::cc_close(krb5_context context, krb5_ccache cache) {
    krb5_cc_close(context, cache);
}

void Krb5InterfaceImpl::free_context(krb5_context context) {
    krb5_free_context(context);
}

const char* Krb5InterfaceImpl::get_error_message(krb5_context context, krb5_error_code code) {
    return krb5_get_error_message(context, code);
}

void Krb5InterfaceImpl::free_error_message(krb5_context context, const char* message) {
    krb5_free_error_message(context, message);
}

Status Krb5InterfaceImpl::unparse_name(krb5_context context, krb5_principal principal,
                                       char** name) {
    krb5_error_code code = krb5_unparse_name(context, principal, name);
    return _check_error(code, context, "Failed to unparse principal name");
}

void Krb5InterfaceImpl::free_unparsed_name(krb5_context context, char* name) {
    krb5_free_unparsed_name(context, name);
}

Status Krb5InterfaceImpl::_check_error(krb5_error_code code, krb5_context context,
                                       const char* message) {
    if (code) {
        const char* err_message = get_error_message(context, code);
        std::string full_message = std::string(message) + ": " + err_message;
        free_error_message(context, err_message);
        return Status::InternalError(full_message);
    }
    return Status::OK();
}

std::unique_ptr<Krb5Interface> Krb5InterfaceFactory::create() {
    return std::make_unique<Krb5InterfaceImpl>();
}

} // namespace doris::kerberos
