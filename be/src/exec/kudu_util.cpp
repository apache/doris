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

#include "exec/kudu_util.h"

#include <string>
#include <sstream>

#include <kudu/client/callbacks.h>
#include <kudu/client/schema.h>

#include "common/logging.h"
// #include "common/names.h"
#include "common/status.h"

using kudu::client::KuduSchema;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;

namespace doris {

bool KuduClientIsSupported() {
  // The value below means the client is actually a stubbed client. This should mean
  // that no official client exists for the underlying OS. The value below should match
  // the value generated in bin/bootstrap_toolchain.py.
  return kudu::client::GetShortVersionString() != "__IMPALA_KUDU_STUB__";
}

bool KuduIsAvailable() { return CheckKuduAvailability().ok(); }

Status CheckKuduAvailability() {
  if (KuduClientIsSupported()) {
    if (config::disable_kudu) {
      return Status::InternalError(TStatusCode::KUDU_NOT_ENABLED);
    } else{
      return Status::OK();
    }
  }
  return Status::InternalError(TStatusCode::KUDU_NOT_SUPPORTED_ON_OS);
}

Status CreateKuduClient(const std::vector<std::string>& master_addrs,
        kudu::client::sp::shared_ptr<KuduClient>* client) {
    kudu::client::KuduClientBuilder b;
    for (const std::string& address: master_addrs) {
        b.add_master_server_addr(address);
    }
    KUDU_RETURN_IF_ERROR(b.Build(client), "Unable to create Kudu client");
    return Status::OK();
}

std::string KuduSchemaDebugString(const KuduSchema& schema) {
  std::stringstream ss;
  for (int i = 0; i < schema.num_columns(); ++i) {
    const KuduColumnSchema& col = schema.Column(i);
    ss << col.name() << " " << KuduColumnSchema::DataTypeToString(col.type()) << "\n";
  }
  return ss.str();
}

void LogKuduMessage(void* unused, kudu::client::KuduLogSeverity severity,
    const char* filename, int line_number, const struct ::tm* time, const char* message,
    size_t message_len) {

  // Note: use raw ints instead of the nice LogSeverity typedef
  // that can be found in glog/log_severity.h as it has an import
  // conflict with gutil/logging-inl.h (indirectly imported).
  int glog_severity;

  switch (severity) {
    case kudu::client::SEVERITY_INFO: glog_severity = 0; break;
    // Log Kudu WARNING messages at the INFO level to avoid contention created by glog
    // locking while flushing WARNING messages.
    case kudu::client::SEVERITY_WARNING: glog_severity = 0; break;
    case kudu::client::SEVERITY_ERROR: glog_severity = 2; break;
    case kudu::client::SEVERITY_FATAL: glog_severity = 3; break;
    default : DCHECK(false) << "Unexpected severity type: " << severity;
  }

  google::LogMessage log_entry(filename, line_number, glog_severity);
  std::string msg(message, message_len);
  log_entry.stream() << msg;
}

void InitKuduLogging() {
  DCHECK(KuduIsAvailable());
  static kudu::client::KuduLoggingFunctionCallback<void*> log_cb(&LogKuduMessage, NULL);
  kudu::client::InstallLoggingCallback(&log_cb);
  kudu::client::SetVerboseLogLevel(FLAGS_v);
}

}  // namespace impala
