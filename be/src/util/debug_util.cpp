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

#include "util/debug_util.h"

#include "common/logging.h"

#include <iomanip>
#include <sstream>

#include "common/logging.h"
#include "gen_cpp/version.h"
#include "runtime/descriptors.h"
#include "runtime/raw_value.h"
#include "runtime/tuple_row.h"
#include "runtime/row_batch.h"
#include "util/cpu_info.h"
#include "gen_cpp/Opcodes_types.h"
#include "gen_cpp/types.pb.h"

#define PRECISION 2
#define KILOBYTE (1024)
#define MEGABYTE (1024 * 1024)
#define GIGABYTE (1024 * 1024 * 1024)

#define SECOND (1000)
#define MINUTE (1000 * 60)
#define HOUR (1000 * 60 * 60)

#define THOUSAND (1000)
#define MILLION (THOUSAND * 1000)
#define BILLION (MILLION * 1000)

namespace google {
namespace glog_internal_namespace_ {
void DumpStackTraceToString(std::string* stacktrace);
}
}

namespace palo {

#define THRIFT_ENUM_OUTPUT_FN_IMPL(E, MAP) \
    std::ostream& operator<<(std::ostream& os, const E::type& e) {\
        std::map<int, const char*>::const_iterator i;\
        i = MAP.find(e);\
        if (i != MAP.end()) {\
            os << i->second;\
        }\
        return os;\
    }

// Macro to stamp out operator<< for thrift enums.  Why doesn't thrift do this?
#define THRIFT_ENUM_OUTPUT_FN(E) THRIFT_ENUM_OUTPUT_FN_IMPL(E, _##E##_VALUES_TO_NAMES)

// Macro to implement Print function that returns string for thrift enums
#define THRIFT_ENUM_PRINT_FN(E) \
    std::string Print##E(const E::type& e) {\
        std::stringstream ss;\
        ss << e;\
        return ss.str();\
    }

THRIFT_ENUM_OUTPUT_FN(TExprOpcode);
THRIFT_ENUM_OUTPUT_FN(TAggregationOp);
THRIFT_ENUM_OUTPUT_FN(THdfsFileFormat);
THRIFT_ENUM_OUTPUT_FN(THdfsCompression);
THRIFT_ENUM_OUTPUT_FN(TStmtType);
THRIFT_ENUM_OUTPUT_FN(QueryState);
THRIFT_ENUM_OUTPUT_FN(TAgentServiceVersion);

THRIFT_ENUM_PRINT_FN(TStmtType);
THRIFT_ENUM_PRINT_FN(QueryState);

THRIFT_ENUM_PRINT_FN(TMetricKind);
THRIFT_ENUM_PRINT_FN(TUnit);

std::string print_id(const TUniqueId& id) {
    std::stringstream out;
    out << std::hex << id.hi << ":" << id.lo;
    return out.str();
}

std::string print_id(const PUniqueId& id) {
    std::stringstream out;
    out << std::hex << id.hi() << ":" << id.lo();
    return out.str();
}

bool parse_id(const std::string& s, TUniqueId* id) {
    DCHECK(id != NULL);

    const char* hi_part = s.c_str();
    char* colon = const_cast<char*>(strchr(hi_part, ':'));

    if (colon == NULL) {
        return false;
    }

    const char* lo_part = colon + 1;
    *colon = '\0';

    char* error_hi = NULL;
    char* error_lo = NULL;
    id->hi = strtoul(hi_part, &error_hi, 16);
    id->lo = strtoul(lo_part, &error_lo, 16);

    bool valid = *error_hi == '\0' && *error_lo == '\0';
    *colon = ':';
    return valid;
}

std::string print_plan_node_type(const TPlanNodeType::type& type) {
    std::map<int, const char*>::const_iterator i;
    i = _TPlanNodeType_VALUES_TO_NAMES.find(type);

    if (i != _TPlanNodeType_VALUES_TO_NAMES.end()) {
        return i->second;
    }

    return "Invalid plan node type";
}

std::string print_tuple(const Tuple* t, const TupleDescriptor& d) {
    if (t == NULL) {
        return "null";
    }

    std::stringstream out;
    out << "(";
    bool first_value = true;

    for (int i = 0; i < d.slots().size(); ++i) {
        SlotDescriptor* slot_d = d.slots()[i];

        if (!slot_d->is_materialized()) {
            continue;
        }

        if (first_value) {
            first_value = false;
        } else {
            out << " ";
        }

        if (t->is_null(slot_d->null_indicator_offset())) {
            out << "null";
        } else {
            std::string value_str;
            RawValue::print_value(
                    t->get_slot(slot_d->tuple_offset()),
                    slot_d->type(),
                    -1,
                    &value_str);
            out << value_str;
        }
    }

    out << ")";
    return out.str();
}

std::string print_row(TupleRow* row, const RowDescriptor& d) {
    std::stringstream out;
    out << "[";

    for (int i = 0; i < d.tuple_descriptors().size(); ++i) {
        if (i != 0) {
            out << " ";
        }
        out << print_tuple(row->get_tuple(i), *d.tuple_descriptors()[i]);
    }

    out << "]";
    return out.str();
}

std::string print_batch(RowBatch* batch) {
    std::stringstream out;

    for (int i = 0; i < batch->num_rows(); ++i) {
        out << print_row(batch->get_row(i), batch->row_desc()) << "\n";
    }

    return out.str();
}

std::string get_build_version(bool compact) {
    std::stringstream ss;
    ss << PALO_BUILD_VERSION
#ifdef NDEBUG
       << " RELEASE"
#else
       << " DEBUG"
#endif
       << " (build " << PALO_BUILD_HASH
       << ")";

    if (!compact) {
        ss << std::endl << "Built on " << PALO_BUILD_TIME << " by " << PALO_BUILD_INFO;
    }

    return ss.str();
}

std::string get_version_string(bool compact) {
    std::stringstream ss;
    ss << " version " << get_build_version(compact);
    return ss.str();
}

std::string get_stack_trace() {
    std::string s;
    google::glog_internal_namespace_::DumpStackTraceToString(&s);
    return s;
}

std::string hexdump(const char* buf, int len) {
    std::stringstream ss;
    ss << std::hex << std::uppercase;
    for (int i = 0; i < len; ++i) {
        ss << std::setfill('0') << std::setw(2) << ((uint16_t)buf[i] & 0xff);
    }
    return ss.str();
}

}
