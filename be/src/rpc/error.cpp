// Copyright (c) 2017, Baidu.com, Inc. All Rights Reserved

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "compat.h"
#include "error.h"
#include "common/logging.h"
#include <iomanip>
#include <unordered_map>

namespace palo {

namespace error { 

struct ErrorInfo {
    int          code;
    const char  *text;
};

ErrorInfo error_info[] = {
    { error::UNPOSSIBLE,                  "But that's unpossible!" },
    { error::EXTERNAL,                    "External error" },
    { error::FAILED_EXPECTATION,          "PALO failed expectation" },
    { error::OK,                          "PALO ok" },
    { error::PROTOCOL_ERROR,              "PALO protocol error" },
    { error::REQUEST_TRUNCATED,           "PALO request truncated" },
    { error::RESPONSE_TRUNCATED,          "PALO response truncated" },
    { error::REQUEST_TIMEOUT,             "PALO request timeout" },
    { error::LOCAL_IO_ERROR,              "PALO local i/o error" },
    { error::BAD_ROOT_LOCATION,           "PALO bad root location" },
    { error::BAD_SCHEMA,                  "PALO bad schema" },
    { error::INVALID_METADATA,            "PALO invalid metadata" },
    { error::BAD_KEY,                     "PALO bad key" },
    { error::METADATA_NOT_FOUND,          "PALO metadata not found" },
    { error::HQL_PARSE_ERROR,             "PALO HQL parse error" },
    { error::FILE_NOT_FOUND,              "PALO file not found" },
    {   error::BLOCK_COMPRESSOR_UNSUPPORTED_TYPE,
        "PALO block compressor unsupported type"
    },
    {   error::BLOCK_COMPRESSOR_INVALID_ARG,
        "PALO block compressor invalid arg"
    },
    {   error::BLOCK_COMPRESSOR_TRUNCATED,
        "PALO block compressor block truncated"
    },
    {   error::BLOCK_COMPRESSOR_BAD_HEADER,
        "PALO block compressor bad block header"
    },
    {   error::BLOCK_COMPRESSOR_BAD_MAGIC,
        "PALO block compressor bad magic string"
    },
    {   error::BLOCK_COMPRESSOR_CHECKSUM_MISMATCH,
        "PALO block compressor block checksum mismatch"
    },
    {   error::BLOCK_COMPRESSOR_DEFLATE_ERROR,
        "PALO block compressor deflate error"
    },
    {   error::BLOCK_COMPRESSOR_INFLATE_ERROR,
        "PALO block compressor inflate error"
    },
    {   error::BLOCK_COMPRESSOR_INIT_ERROR,
        "PALO block compressor initialization error"
    },
    { error::TABLE_NOT_FOUND,             "PALO table does not exist" },
    { error::COMMAND_PARSE_ERROR,         "PALO command parse error" },
    { error::CONNECT_ERROR_MASTER,        "PALO Master connect error" },
    {   error::CONNECT_ERROR_HYPERSPACE,
        "PALO Hyperspace client connect error"
    },
    { error::TOO_MANY_COLUMNS,            "PALO too many columns" },
    { error::BAD_DOMAIN_NAME,             "PALO bad domain name" },
    { error::MALFORMED_REQUEST,           "PALO malformed request" },
    { error::BAD_MEMORY_ALLOCATION,       "PALO bad memory allocation"},
    { error::BAD_SCAN_SPEC,               "PALO bad scan specification"},
    { error::NOT_IMPLEMENTED,             "PALO not implemented"},
    { error::VERSION_MISMATCH,            "PALO version mismatch"},
    { error::CANCELLED,                   "PALO cancelled"},
    { error::SCHEMA_PARSE_ERROR,          "PALO schema parse error" },
    { error::SYNTAX_ERROR,                "PALO syntax error" },
    { error::DOUBLE_UNGET,                  "PALO double unget" },
    { error::EMPTY_BLOOMFILTER,             "PALO empty bloom filter" },
    { error::BLOOMFILTER_CHECKSUM_MISMATCH, "PALO bloom filter checksum mismatch" },
    { error::NAME_ALREADY_IN_USE,           "PALO name already in use" },
    { error::NAMESPACE_DOES_NOT_EXIST,      "PALO namespace does not exist" },
    { error::BAD_NAMESPACE,                 "PALO bad namespace" },
    { error::NAMESPACE_EXISTS,              "PALO namespace exists" },
    { error::NO_RESPONSE,                   "PALO no response" },
    { error::NOT_ALLOWED,                   "PALO not allowed" },
    { error::INDUCED_FAILURE,               "PALO induced failure" },
    { error::SERVER_SHUTTING_DOWN,          "PALO server shutting down" },
    { error::LOCATION_UNASSIGNED,           "PALO location unassigned" },
    { error::ALREADY_EXISTS,                "PALO cell already exists" },
    { error::CHECKSUM_MISMATCH,             "PALO checksum mismatch" },
    { error::CLOSED,                        "PALO closed" },
    { error::RANGESERVER_NOT_FOUND,         "PALO RangeServer not found" },
    { error::CONNECTION_NOT_INITIALIZED,    "PALO connection not initialized" },
    { error::DUPLICATE_RANGE,               "PALO duplicate range" },
    { error::INVALID_PSEUDO_TABLE_NAME,     "PALO invalid pseudo-table name" },
    { error::BAD_FORMAT,                    "PALO bad format" },
    { error::INVALID_ARGUMENT,              "PALO invalid argument" },
    { error::INVALID_OPERATION,             "PALO invalid operation" },
    { error::UNSUPPORTED_OPERATION,         "PALO unsupported operation" },
    { error::COLUMN_FAMILY_NOT_FOUND,       "PALO column family not found" },
    { error::NOTHING_TO_DO,                 "PALO nothing to do" },
    { error::INCOMPATIBLE_OPTIONS,          "PALO incompatible options" },
    { error::BAD_VALUE,                     "PALO bad value" },
    { error::SCHEMA_GENERATION_MISMATCH,    "PALO schema generation mismatch" },
    { error::INVALID_METHOD_IDENTIFIER,     "PALO invalid method identifier" },
    { error::SERVER_NOT_READY,              "PALO server not ready" },
    { error::CONFIG_BAD_ARGUMENT,         "CONFIG bad argument(s)"},
    { error::CONFIG_BAD_CFG_FILE,         "CONFIG bad cfg file"},
    { error::CONFIG_GET_ERROR,            "CONFIG failed to get config value"},
    { error::CONFIG_BAD_VALUE,            "CONFIG bad config value"},
    { error::COMM_NOT_CONNECTED,          "COMM not connected" },
    { error::COMM_BROKEN_CONNECTION,      "COMM broken connection" },
    { error::COMM_CONNECT_ERROR,          "COMM connect error" },
    { error::COMM_ALREADY_CONNECTED,      "COMM already connected" },
    { error::COMM_SEND_ERROR,             "COMM send error" },
    { error::COMM_RECEIVE_ERROR,          "COMM receive error" },
    { error::COMM_POLL_ERROR,             "COMM poll error" },
    { error::COMM_CONFLICTING_ADDRESS,    "COMM conflicting address" },
    { error::COMM_SOCKET_ERROR,           "COMM socket error" },
    { error::COMM_BIND_ERROR,             "COMM bind error" },
    { error::COMM_LISTEN_ERROR,           "COMM listen error" },
    { error::COMM_HEADER_CHECKSUM_MISMATCH,  "COMM header checksum mismatch" },
    { error::COMM_PAYLOAD_CHECKSUM_MISMATCH, "COMM payload checksum mismatch" },
    { error::COMM_BAD_HEADER,             "COMM bad header" },
    { error::COMM_INVALID_PROXY,          "COMM invalid proxy" },
    { error::FSBROKER_BAD_FILE_HANDLE,   "FS BROKER bad file handle" },
    { error::FSBROKER_IO_ERROR,          "FS BROKER i/o error" },
    { error::FSBROKER_FILE_NOT_FOUND,    "FS BROKER file not found" },
    { error::FSBROKER_BAD_FILENAME,      "FS BROKER bad filename" },
    { error::FSBROKER_PERMISSION_DENIED, "FS BROKER permission denied" },
    { error::FSBROKER_INVALID_ARGUMENT,  "FS BROKER invalid argument" },
    { error::FSBROKER_INVALID_CONFIG,    "FS BROKER invalid config value" },
    { error::FSBROKER_EOF,               "FS BROKER end of file" },
    { error::HYPERSPACE_IO_ERROR,          "HYPERSPACE i/o error" },
    { error::HYPERSPACE_CREATE_FAILED,     "HYPERSPACE create failed" },
    { error::HYPERSPACE_FILE_NOT_FOUND,    "HYPERSPACE file not found" },
    { error::HYPERSPACE_ATTR_NOT_FOUND,    "HYPERSPACE attribute not found" },
    { error::HYPERSPACE_DELETE_ERROR,      "HYPERSPACE delete error" },
    { error::HYPERSPACE_BAD_PATHNAME,      "HYPERSPACE bad pathname" },
    { error::HYPERSPACE_PERMISSION_DENIED, "HYPERSPACE permission denied" },
    { error::HYPERSPACE_EXPIRED_SESSION,   "HYPERSPACE expired session" },
    { error::HYPERSPACE_FILE_EXISTS,       "HYPERSPACE file exists" },
    { error::HYPERSPACE_IS_DIRECTORY,      "HYPERSPACE is directory" },
    { error::HYPERSPACE_INVALID_HANDLE,    "HYPERSPACE invalid handle" },
    { error::HYPERSPACE_REQUEST_CANCELLED, "HYPERSPACE request cancelled" },
    { error::HYPERSPACE_MODE_RESTRICTION,  "HYPERSPACE mode restriction" },
    { error::HYPERSPACE_ALREADY_LOCKED,    "HYPERSPACE already locked" },
    { error::HYPERSPACE_LOCK_CONFLICT,     "HYPERSPACE lock conflict" },
    { error::HYPERSPACE_NOT_LOCKED,        "HYPERSPACE not locked" },
    { error::HYPERSPACE_BAD_ATTRIBUTE,     "HYPERSPACE bad attribute" },
    { error::HYPERSPACE_BERKELEYDB_ERROR,  "HYPERSPACE Berkeley DB error" },
    { error::HYPERSPACE_DIR_NOT_EMPTY,     "HYPERSPACE directory not empty" },
    {   error::HYPERSPACE_BERKELEYDB_DEADLOCK,
        "HYPERSPACE Berkeley DB deadlock"
    },
    {   error::HYPERSPACE_BERKELEYDB_REP_HANDLE_DEAD,
        "HYPERSPACE Berkeley DB replication handle dead"
    },
    { error::HYPERSPACE_FILE_OPEN,        "HYPERSPACE file open" },
    { error::HYPERSPACE_CLI_PARSE_ERROR,  "HYPERSPACE CLI parse error" },
    {   error::HYPERSPACE_CREATE_SESSION_FAILED,
        "HYPERSPACE unable to create session "
    },
    {   error::HYPERSPACE_NOT_MASTER_LOCATION,
        "HYPERSPACE not master location"
    },
    {   error::HYPERSPACE_STATEDB_ERROR,
        "HYPERSPACE State DB error"
    },
    {   error::HYPERSPACE_STATEDB_DEADLOCK,
        "HYPERSPACE State DB deadlock"
    },
    {   error::HYPERSPACE_STATEDB_BAD_KEY,
        "HYPERSPACE State DB bad key"
    },
    {   error::HYPERSPACE_STATEDB_BAD_VALUE,
        "HYPERSPACE State DB bad value"
    },
    {   error::HYPERSPACE_STATEDB_ALREADY_DELETED,
        "HYPERSPACE State DB attempt to access/delete previously deleted state"
    },
    {   error::HYPERSPACE_STATEDB_EVENT_EXISTS,
        "HYPERSPACE State DB event exists"
    },
    {   error::HYPERSPACE_STATEDB_EVENT_NOT_EXISTS,
        "HYPERSPACE State DB event does not exist"
    },
    {   error::HYPERSPACE_STATEDB_EVENT_ATTR_NOT_FOUND,
        "HYPERSPACE State DB event attr not found"
    },
    {   error::HYPERSPACE_STATEDB_SESSION_EXISTS,
        "HYPERSPACE State DB session exists"
    },
    {   error::HYPERSPACE_STATEDB_SESSION_NOT_EXISTS,
        "HYPERSPACE State DB session does not exist"
    },
    {   error::HYPERSPACE_STATEDB_SESSION_ATTR_NOT_FOUND,
        "HYPERSPACE State DB session attr not found"
    },
    {   error::HYPERSPACE_STATEDB_HANDLE_EXISTS,
        "HYPERSPACE State DB handle exists"
    },
    {   error::HYPERSPACE_STATEDB_HANDLE_NOT_EXISTS,
        "HYPERSPACE State DB handle does not exist"
    },
    {   error::HYPERSPACE_STATEDB_HANDLE_ATTR_NOT_FOUND,
        "HYPERSPACE State DB handle attr not found"
    },
    {   error::HYPERSPACE_STATEDB_NODE_EXISTS,
        "HYPERSPACE State DB node exists"
    },
    {   error::HYPERSPACE_STATEDB_NODE_NOT_EXISTS,
        "HYPERSPACE State DB node does not exist"
    },
    {   error::HYPERSPACE_STATEDB_NODE_ATTR_NOT_FOUND,
        "HYPERSPACE State DB node attr not found"
    },
    {   error::HYPERSPACE_VERSION_MISMATCH,
        "HYPERSPACE client/server protocol version mismatch"
    },
    { error::MASTER_TABLE_EXISTS,         "MASTER table exists" },
    { error::MASTER_BAD_SCHEMA,           "MASTER bad schema" },
    { error::MASTER_NOT_RUNNING,          "MASTER not running" },
    { error::MASTER_NO_RANGESERVERS,      "MASTER no range servers" },
    { error::MASTER_FILE_NOT_LOCKED,      "MASTER file not locked" },
    {   error::MASTER_RANGESERVER_ALREADY_REGISTERED,
        "MASTER range server with same location already registered"
    },
    { error::MASTER_BAD_COLUMN_FAMILY,    "MASTER bad column family" },
    {   error::MASTER_SCHEMA_GENERATION_MISMATCH,
        "Master schema generation mismatch"
    },
    {   error::MASTER_LOCATION_ALREADY_ASSIGNED,
        "MASTER location already assigned"
    },
    { error::MASTER_LOCATION_INVALID, "MASTER location invalid" },
    { error::MASTER_OPERATION_IN_PROGRESS, "MASTER operation in progress" },
    { error::MASTER_RANGESERVER_IN_RECOVERY, "MASTER RangeServer in recovery" },
    { error::MASTER_BALANCE_PREVENTED, "MASTER balance operation prevented" },
    {   error::RANGESERVER_GENERATION_MISMATCH,
        "RANGE SERVER generation mismatch"
    },
    {   error::RANGESERVER_RANGE_ALREADY_LOADED,
        "RANGE SERVER range already loaded"
    },
    { error::RANGESERVER_RANGE_MISMATCH,       "RANGE SERVER range mismatch" },
    {   error::RANGESERVER_NONEXISTENT_RANGE,
        "RANGE SERVER non-existent range"
    },
    { error::RANGESERVER_OUT_OF_RANGE,         "RANGE SERVER out of range" },
    { error::RANGESERVER_RANGE_NOT_FOUND,      "RANGE SERVER range not found" },
    {   error::RANGESERVER_INVALID_SCANNER_ID,
        "RANGE SERVER invalid scanner id"
    },
    {   error::RANGESERVER_SCHEMA_PARSE_ERROR,
        "RANGE SERVER schema parse error"
    },
    {   error::RANGESERVER_SCHEMA_INVALID_CFID,
        "RANGE SERVER invalid column family id"
    },
    {   error::RANGESERVER_INVALID_COLUMNFAMILY,
        "RANGE SERVER invalid column family"
    },
    {   error::RANGESERVER_TRUNCATED_COMMIT_LOG,
        "RANGE SERVER truncated commit log"
    },
    {   error::RANGESERVER_NO_METADATA_FOR_RANGE,
        "RANGE SERVER no metadata for range"
    },
    { error::RANGESERVER_SHUTTING_DOWN,        "RANGE SERVER shutting down" },
    {   error::RANGESERVER_CORRUPT_COMMIT_LOG,
        "RANGE SERVER corrupt commit log"
    },
    { error::RANGESERVER_UNAVAILABLE,          "RANGE SERVER unavailable" },
    {   error::RANGESERVER_REVISION_ORDER_ERROR,
        "RANGE SERVER supplied revision is not strictly increasing"
    },
    { error::RANGESERVER_ROW_OVERFLOW,         "RANGE SERVER row overflow" },
    { error::RANGESERVER_TABLE_NOT_FOUND,      "RANGE SERVER table not found" },
    {   error::RANGESERVER_BAD_SCAN_SPEC,
        "RANGE SERVER bad scan specification"
    },
    {   error::RANGESERVER_CLOCK_SKEW,
        "RANGE SERVER clock skew detected"
    },
    {   error::RANGESERVER_BAD_CELLSTORE_FILENAME,
        "RANGE SERVER bad CellStore filename"
    },
    {   error::RANGESERVER_CORRUPT_CELLSTORE,
        "RANGE SERVER corrupt CellStore"
    },
    { error::RANGESERVER_TABLE_DROPPED, "RANGE SERVER table dropped" },
    { error::RANGESERVER_UNEXPECTED_TABLE_ID, "RANGE SERVER unexpected table ID" },
    { error::RANGESERVER_RANGE_BUSY, "RANGE SERVER range busy" },
    { error::RANGESERVER_BAD_CELL_INTERVAL, "RANGE SERVER bad cell interval" },
    { error::RANGESERVER_SHORT_CELLSTORE_READ, "RANGE SERVER short cellstore read" },
    { error::RANGESERVER_RANGE_NOT_ACTIVE, "RANGE SERVER range no longer active" },
    {   error::RANGESERVER_FRAGMENT_ALREADY_PROCESSED,
        "RANGE SERVER fragment already processed"
    },
    {   error::RANGESERVER_RECOVERY_PLAN_GENERATION_MISMATCH,
        "RANGE SERVER recovery plan generation mismatch"
    },
    {   error::RANGESERVER_PHANTOM_RANGE_MAP_NOT_FOUND,
        "RANGE SERVER phantom range map not found"
    },
    {   error::RANGESERVER_RANGES_ALREADY_LIVE,
        "RANGE SERVER ranges already live"
    },
    {   error::RANGESERVER_RANGE_NOT_YET_ACKNOWLEDGED,
        "RANGE SERVER range not yet acknowledged"
    },
    {   error::RANGESERVER_SERVER_IN_READONLY_MODE,
        "RANGE SERVER server in readonly mode"
    },
    {   error::RANGESERVER_RANGE_NOT_YET_RELINQUISHED,
        "RANGE SERVER range not yet relinquished"
    },
    { error::HQL_BAD_LOAD_FILE_FORMAT,         "HQL bad load file format" },
    { error::HQL_BAD_COMMAND, "HQL bad command" },
    { error::METALOG_VERSION_MISMATCH, "METALOG version mismatch" },
    { error::METALOG_BAD_RS_HEADER, "METALOG bad range server metalog header" },
    { error::METALOG_BAD_HEADER,  "METALOG bad metalog header" },
    { error::METALOG_ENTRY_TRUNCATED,   "METALOG entry truncated" },
    { error::METALOG_CHECKSUM_MISMATCH, "METALOG checksum mismatch" },
    { error::METALOG_ENTRY_BAD_TYPE, "METALOG bad entry type" },
    { error::METALOG_ENTRY_BAD_ORDER, "METALOG entry out of order" },
    { error::METALOG_MISSING_RECOVER_ENTITY, "METALOG missing RECOVER entity" },
    { error::METALOG_BACKUP_FILE_MISMATCH, "METALOG backup file mismatch" },
    { error::METALOG_READ_ERROR, "METALOG read error" },
    {   error::SERIALIZATION_INPUT_OVERRUN,
        "SERIALIZATION input buffer overrun"
    },
    { error::SERIALIZATION_BAD_VINT,      "SERIALIZATION bad vint encoding" },
    { error::SERIALIZATION_BAD_VSTR,      "SERIALIZATION bad vstr encoding" },
    { error::SERIALIZATION_VERSION_MISMATCH, "SERIALIZATION version mismatch" },
    { error::THRIFTBROKER_BAD_SCANNER_ID, "THRIFT BROKER bad scanner id" },
    { error::THRIFTBROKER_BAD_MUTATOR_ID, "THRIFT BROKER bad mutator id" },
    { error::THRIFTBROKER_BAD_NAMESPACE_ID, "THRIFT BROKER bad namespace id" },
    { error::THRIFTBROKER_BAD_FUTURE_ID,    "THRIFT BROKER bad future id" },
    { 0, 0 }
};

typedef std::unordered_map<int, const char *>  TextMap;
TextMap &build_text_map() {
    TextMap *map = new TextMap();
    for (int i = 0; error_info[i].text != 0; i++) {
        (*map)[error_info[i].code] = error_info[i].text;
    }
    return *map;
}

TextMap &text_map = build_text_map();

const char* get_text(int error) {
    const char *text = text_map[error];
    if (text == 0) {
        return "ERROR NOT REGISTERED";
    }
    return text;
}

void generate_html_error_code_documentation(std::ostream &out) {
    out << "<table border=\"1\" cellpadding=\"4\" cellspacing=\"1\" style=\"width: 720px; \">\n";
    out << "<thead><tr><th scope=\"col\">Code<br />(hexidecimal)</th>\n";
    out << "<th scope=\"col\">Code<br />(decimal)</th>\n";
    out << "<th scope=\"col\">Description</th></tr></thead><tbody>\n";
    for (size_t i = 0; error_info[i].text; i++) {
        if (error_info[i].code >= 0) {
            out << "<tr><td style=\"text-align: right; \"><code>0x" 
                << std::hex << error_info[i].code << "</code></td>\n";
        } else {
            out << "<tr><td style=\"text-align: right; \"><code></code></td>\n";
        }
        out << "<td style=\"text-align: right; \"><code>" 
            << std::dec << error_info[i].code << "</code></td>\n";
        out << "<td>" << error_info[i].text << "</td></tr>\n";
    }
    out << "</tbody></table>\n" << std::flush;
}

} //namespace error

const char *relative_fname(const Exception &e) {
    if (e.file()) {
        const char *ptr = strstr(e.file(), "src/cc/");
        return ptr ? ptr : e.file();
    }
    return "";
}

std::ostream &operator<<(std::ostream &out, const Exception &e) {
    out << "palo::Exception: " << e.message() << " - "
        << error::get_text(e.code());
    if (e.line()) {
        out << "\n\tat " << e.func() << " (";
        //if (Logger::get()->show_line_numbers())
        if (true) {
            out << e.file() << ':' << e.line();
        } else {
            out << relative_fname(e);
        }
        out << ')';
    }
    int prev_code = e.code();
    for (Exception *prev = e.prev; prev; prev = prev->prev) {
        out << "\n\tat " << (prev->func() ? prev->func() : "-") << " (";
        //if (Logger::get()->show_line_numbers())
        if (true) {
            out << (prev->file() ? prev->file() : "-") << ':' << prev->line();
        } else {
            out << relative_fname(*prev);
        }
        out << "): " << prev->message();
        if (prev->code() != prev_code) {
            out << " - " << error::get_text(prev->code());
            prev_code = prev->code();
        }
    }
    return out;
}

std::ostream &
Exception::render_messages(std::ostream &out, const char *sep) const {
    out << message() << " - " << error::get_text(m_error);
    for (Exception *p = prev; p; p = p->prev) {
        out << sep << p->message();
    }
    return out;
}

} // namespace palo
