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

#include "load/routine_load/kafka_properties.h"

#include <gen_cpp/BackendService_constants.h>
#include <librdkafka/rdkafkacpp.h>

#include "common/status.h"
#include "util/string_util.h"

namespace doris {

Status configure_kafka_security(RdKafka::Conf& conf,
                                const std::map<std::string, std::string>& properties) {
    for (const auto& [key, value] : properties) {
        // A new FE also sends zero explicitly to protect rolling upgrades to old BEs.
        // Never allow a user to re-enable the default kinit command with a nonzero interval.
        if (key == "sasl.kerberos.min.time.before.relogin" && value != "0") {
            return Status::InvalidArgument(
                    "PAUSE: Kafka property '{}' is not allowed in Routine Load with a nonzero "
                    "value; "
                    "Kerberos tickets must be managed externally",
                    key);
        }
        // Certificate paths supplied by SQL users must be Doris-managed files.
        if (g_BackendService_constants.KAFKA_FILE_PROPERTIES.contains(key)) {
            if (!starts_with(value, "FILE:") || value.size() == 5) {
                return Status::InvalidArgument(
                        "PAUSE: Kafka property '{}' must reference a file registered with CREATE "
                        "FILE",
                        key);
            }
        } else if (!g_BackendService_constants.KAFKA_ALLOWED_PROPERTIES.contains(key) &&
                   !g_BackendService_constants.KAFKA_DORIS_PROPERTIES.contains(key)) {
            // For example, plugin.library.paths can load code during Conf::set(),
            // before a consumer is even created. Unknown properties fail closed.
            return Status::InvalidArgument(
                    "PAUSE: Kafka property '{}' is not allowed in Routine Load", key);
        }
    }

    // Rejecting kinit.cmd alone is insufficient: the default command interpolates
    // the principal/keytab. Setting the interval to zero disables both the initial
    // command and the refresh timer. Operators must maintain the ticket cache.
    // This is mandatory, so do not use init's best-effort set_conf wrapper here.
    std::string error;
    if (conf.set("sasl.kerberos.min.time.before.relogin", "0", error) != RdKafka::Conf::CONF_OK) {
        return Status::InternalError(
                "PAUSE: failed to disable Kafka Kerberos command execution: {}", error);
    }
    return Status::OK();
}

} // namespace doris
