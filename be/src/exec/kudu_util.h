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

#ifndef IMPALA_UTIL_KUDU_UTIL_H_
#define IMPALA_UTIL_KUDU_UTIL_H_

#include <kudu/client/callbacks.h>
#include <kudu/client/client.h>

namespace doris {

class Status;

/// Returns false when running on an operating system that Kudu doesn't support. If this
/// check fails, there is no way Kudu should be expected to work. Exposed for testing.
bool KuduClientIsSupported();

/// Returns OK if Kudu is available or an error status containing the reason Kudu is not
/// available. Kudu may not be available if no Kudu client is available for the platform
/// or if Kudu was disabled by the startup flag --disable_kudu.
Status CheckKuduAvailability();

/// Convenience function for the bool equivalent of CheckKuduAvailability().
bool KuduIsAvailable();

/// Creates a new KuduClient using the specified master adresses. If any error occurs,
/// 'client' is not set and an error status is returned.
Status CreateKuduClient(const std::vector<std::string>& master_addrs,
    kudu::client::sp::shared_ptr<kudu::client::KuduClient>* client);

/// Returns a debug string for the KuduSchema.
std::string KuduSchemaDebugString(const kudu::client::KuduSchema& schema);

/// Initializes Kudu's logging by binding a callback that logs back to Impala's glog. This
/// also sets Kudu's verbose logging to whatever level is set in Impala.
void InitKuduLogging();

// This is the callback mentioned above. When the Kudu client logs a message it gets
// redirected here and forwarded to Impala's glog.
// This method is not supposed to be used directly.
void LogKuduMessage(kudu::client::KuduLogSeverity severity, const char* filename,
    int line_number, const struct ::tm* time, const char* message, size_t message_len);

/// Takes a Kudu status and returns an impala one, if it's not OK.
#define KUDU_RETURN_IF_ERROR(expr, prepend) \
  do { \
    kudu::Status _s = (expr); \
    if (UNLIKELY(!_s.ok())) {                                      \
      return Status::InternalError(_s.ToString()); \
    } \
  } while (0)


 // 63: return Status::InternalError("strings::Substitute("$0: $1", prepend, _s.ToString()));

} /// namespace impala
#endif
