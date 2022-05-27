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

#include <mysql/mysql.h>

#define __DorisMysql MYSQL
#include "mysql_load_error_hub.h"
#include "util/defer_op.h"

namespace doris {

MysqlLoadErrorHub::MysqlLoadErrorHub(const TMysqlErrorHubInfo& info) : _info(info) {}

MysqlLoadErrorHub::~MysqlLoadErrorHub() {}

Status MysqlLoadErrorHub::prepare() {
    _is_valid = true;
    return Status::OK();
}

Status MysqlLoadErrorHub::export_error(const ErrorMsg& error_msg) {
    std::lock_guard<std::mutex> lock(_mtx);
    ++_total_error_num;

    if (!_is_valid) {
        return Status::OK();
    }

    _error_msgs.push(error_msg);
    if (_error_msgs.size() >= EXPORTER_THRESHOLD) {
        RETURN_IF_ERROR(write_mysql());
    }

    return Status::OK();
}

Status MysqlLoadErrorHub::close() {
    std::lock_guard<std::mutex> lock(_mtx);

    if (!_is_valid) {
        return Status::OK();
    }

    if (!_error_msgs.empty()) {
        RETURN_IF_ERROR(write_mysql());
    }

    return Status::OK();
}

Status MysqlLoadErrorHub::write_mysql() {
    MYSQL* my_conn = nullptr;
    Status st = open_mysql_conn(&my_conn);
    if (!st.ok()) {
        _is_valid = false;
        return st;
    }

    Defer close_mysql_conn {[=]() { mysql_close(my_conn); }};

    Status status;
    std::stringstream sql_stream;
    while (!_error_msgs.empty()) {
        status = gen_sql(my_conn, _error_msgs.front(), &sql_stream);
        if (!status.ok()) {
            return error_status("fail to gen sql", my_conn);
        }
        _error_msgs.pop();
    }

    int sql_result = mysql_query(my_conn, sql_stream.str().c_str());
    if (sql_result != 0) {
        LOG(WARNING) << "mysql insert failed. query=[" << sql_stream.str() << "]";
        return error_status("mysql query failed.", my_conn);
    }

    VLOG_PROGRESS << "mysql query success. query =" << sql_stream.str();

    return Status::OK();
}

Status MysqlLoadErrorHub::gen_sql(MYSQL* my_conn, const LoadErrorHub::ErrorMsg& error_msg,
                                  std::stringstream* sql_stream) {
    char* sql_start = &_escape_buff[0];
    size_t msg_size = error_msg.msg.size();
    if (msg_size > EXPORTER_MAX_LINE_SIZE) {
        msg_size = EXPORTER_MAX_LINE_SIZE;
    }

    (*sql_stream) << "insert into " << _info.table << " (job_id, error_msg) values("
                  << error_msg.job_id << ", '" << sql_start << "'); ";
    return Status::OK();
}

Status MysqlLoadErrorHub::open_mysql_conn(MYSQL** my_conn) {
    *my_conn = mysql_init(nullptr);
    if (nullptr == *my_conn) {
        LOG(WARNING) << "load error export's mysql init failed.";
        return Status::InternalError("mysql init failed.");
    }
    VLOG_ROW << "MysqlLoadErrorHub::init";

    if (!mysql_real_connect(*my_conn, _info.host.c_str(), _info.user.c_str(), _info.passwd.c_str(),
                            _info.db.c_str(), _info.port, nullptr, CLIENT_MULTI_STATEMENTS)) {
        LOG(WARNING) << "fail to connect Mysql: "
                     << "Host: " << _info.host << " port: " << _info.port << " user: " << _info.user
                     << " passwd: " << _info.passwd << " db: " << _info.db;
        return error_status("load error mysql real connect failed.", *my_conn);
    }

    return Status::OK();
}

Status MysqlLoadErrorHub::error_status(const std::string& prefix, MYSQL* my_conn) {
    std::stringstream msg;
    msg << prefix << " Err: " << mysql_error(my_conn);
    LOG(WARNING) << msg.str();
    return Status::InternalError(msg.str());
}

std::string MysqlLoadErrorHub::debug_string() const {
    std::stringstream out;
    out << "(total_error_num=" << _total_error_num << ")";
    return out.str();
}

} // end namespace doris
