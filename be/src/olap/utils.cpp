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

#include "olap/utils.h"

// IWYU pragma: no_include <bthread/errno.h>
#include <errno.h> // IWYU pragma: keep
#include <stdarg.h>
#include <time.h>
#include <unistd.h>
#include <zconf.h>
#include <zlib.h>

#include <cmath>
#include <cstring>
#include <memory>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include "common/logging.h"
#include "common/status.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_writer.h"
#include "io/fs/local_file_system.h"
#include "olap/olap_common.h"
#include "util/sse_util.hpp"
#include "util/string_parser.hpp"
#include "vec/runtime/ipv4_value.h"
#include "vec/runtime/ipv6_value.h"

namespace doris {
using namespace ErrorCode;

uint32_t olap_adler32_init() {
    return adler32(0L, Z_NULL, 0);
}

uint32_t olap_adler32(uint32_t adler, const char* buf, size_t len) {
    return adler32(adler, reinterpret_cast<const Bytef*>(buf), len);
}

Status gen_timestamp_string(std::string* out_string) {
    time_t now = time(nullptr);
    tm local_tm;

    if (localtime_r(&now, &local_tm) == nullptr) {
        return Status::Error<OS_ERROR>("fail to localtime_r time. time={}", now);
    }
    char time_suffix[16] = {0}; // Example: 20150706111404's length is 15
    if (strftime(time_suffix, sizeof(time_suffix), "%Y%m%d%H%M%S", &local_tm) == 0) {
        return Status::Error<OS_ERROR>("fail to strftime time. time={}", now);
    }

    *out_string = time_suffix;
    return Status::OK();
}

Status read_write_test_file(const std::string& test_file_path) {
    if (access(test_file_path.c_str(), F_OK) == 0) {
        if (remove(test_file_path.c_str()) != 0) {
            char errmsg[64];
            return Status::IOError("fail to access test file. path={}, errno={}, err={}",
                                   test_file_path, errno, strerror_r(errno, errmsg, 64));
        }
    } else {
        if (errno != ENOENT) {
            char errmsg[64];
            return Status::IOError("fail to access test file. path={}, errno={}, err={}",
                                   test_file_path, errno, strerror_r(errno, errmsg, 64));
        }
    }

    const size_t TEST_FILE_BUF_SIZE = 4096;
    const size_t DIRECT_IO_ALIGNMENT = 512;
    char* write_test_buff = nullptr;
    char* read_test_buff = nullptr;
    if (posix_memalign((void**)&write_test_buff, DIRECT_IO_ALIGNMENT, TEST_FILE_BUF_SIZE) != 0) {
        return Status::Error<MEM_ALLOC_FAILED>("fail to allocate write buffer memory. size={}",
                                               TEST_FILE_BUF_SIZE);
    }
    std::unique_ptr<char, decltype(&std::free)> write_buff(write_test_buff, &std::free);
    if (posix_memalign((void**)&read_test_buff, DIRECT_IO_ALIGNMENT, TEST_FILE_BUF_SIZE) != 0) {
        return Status::Error<MEM_ALLOC_FAILED>("fail to allocate read buffer memory. size={}",
                                               TEST_FILE_BUF_SIZE);
    }
    std::unique_ptr<char, decltype(&std::free)> read_buff(read_test_buff, &std::free);
    // generate random numbers
    uint32_t rand_seed = static_cast<uint32_t>(time(nullptr));
    for (size_t i = 0; i < TEST_FILE_BUF_SIZE; ++i) {
        int32_t tmp_value = rand_r(&rand_seed);
        write_test_buff[i] = static_cast<char>(tmp_value);
    }

    // write file
    io::FileWriterPtr file_writer;
    RETURN_IF_ERROR(io::global_local_filesystem()->create_file(test_file_path, &file_writer));
    RETURN_IF_ERROR(file_writer->append({write_buff.get(), TEST_FILE_BUF_SIZE}));
    RETURN_IF_ERROR(file_writer->close());
    // read file
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(io::global_local_filesystem()->open_file(test_file_path, &file_reader));
    size_t bytes_read = 0;
    RETURN_IF_ERROR(file_reader->read_at(0, {read_buff.get(), TEST_FILE_BUF_SIZE}, &bytes_read));
    if (memcmp(write_buff.get(), read_buff.get(), TEST_FILE_BUF_SIZE) != 0) {
        return Status::IOError("the test file write_buf and read_buf not equal, file_name={}.",
                               test_file_path);
    }
    // delete file
    return io::global_local_filesystem()->delete_file(test_file_path);
}

Status check_datapath_rw(const std::string& path) {
    bool exists = true;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(path, &exists));
    if (!exists) {
        return Status::IOError("path does not exist: {}", path);
    }
    std::string file_path = path + "/.read_write_test_file";
    return read_write_test_file(file_path);
}

__thread char Errno::_buf[BUF_SIZE]; ///< buffer instance

const char* Errno::str() {
    return str(no());
}

const char* Errno::str(int no) {
    if (0 != strerror_r(no, _buf, BUF_SIZE)) {
        LOG(WARNING) << "fail to get errno string. [no='" << no << "', errno='" << errno << "']";
        snprintf(_buf, BUF_SIZE, "unknown errno");
    }

    return _buf;
}

int Errno::no() {
    return errno;
}

template <>
bool valid_signed_number<int128_t>(const std::string& value_str) {
    char* endptr = nullptr;
    const char* value_string = value_str.c_str();
    int64_t value = strtol(value_string, &endptr, 10);
    if (*endptr != 0) {
        return false;
    } else if (value > LONG_MIN && value < LONG_MAX) {
        return true;
    } else {
        bool sign = false;
        if (*value_string == '-' || *value_string == '+') {
            if (*(value_string++) == '-') {
                sign = true;
            }
        }

        uint128_t current = 0;
        uint128_t max_int128 = std::numeric_limits<int128_t>::max();
        while (*value_string != 0) {
            if (current > max_int128 / 10) {
                return false;
            }

            current = current * 10 + (*(value_string++) - '0');
        }

        if ((!sign && current > max_int128) || (sign && current > max_int128 + 1)) {
            return false;
        }

        return true;
    }
}

bool valid_decimal(const std::string& value_str, const uint32_t precision, const uint32_t frac) {
    const char* decimal_pattern = "-?(\\d+)(.\\d+)?";
    std::regex e(decimal_pattern);
    std::smatch what;
    if (!std::regex_match(value_str, what, e) || what[0].str().size() != value_str.size()) {
        LOG(WARNING) << "invalid decimal value. [value=" << value_str << "]";
        return false;
    }

    size_t number_length = value_str.size();
    bool is_negative = value_str[0] == '-';
    if (is_negative) {
        --number_length;
    }

    size_t integer_len = 0;
    size_t fractional_len = 0;
    size_t point_pos = value_str.find('.');
    if (point_pos == std::string::npos) {
        integer_len = number_length;
        fractional_len = 0;
    } else {
        integer_len = point_pos - (is_negative ? 1 : 0);
        fractional_len = number_length - point_pos - 1;
    }

    /// For value likes "0.xxxxxx", the integer_len should actually be 0.
    if (integer_len == 1 && precision - frac == 0) {
        if (what[1].str() == "0") {
            integer_len = 0;
        }
    }

    return (integer_len <= (precision - frac) && fractional_len <= frac);
}

bool valid_datetime(const std::string& value_str, const uint32_t scale) {
    const char* datetime_pattern =
            "((?:\\d){4})-((?:\\d){2})-((?:\\d){2})[ ]*"
            "(((?:\\d){2}):((?:\\d){2}):((?:\\d){2})([.]*((?:\\d){0,6})))?";
    std::regex e(datetime_pattern);
    std::smatch what;

    if (std::regex_match(value_str, what, e)) {
        if (what[0].str().size() != value_str.size()) {
            LOG(WARNING) << "datetime str does not fully match. [value_str=" << value_str
                         << " match=" << what[0].str() << "]";
            return false;
        }

        int month = strtol(what[2].str().c_str(), nullptr, 10);
        if (month < 1 || month > 12) {
            LOG(WARNING) << "invalid month. [month=" << month << "]";
            return false;
        }

        int day = strtol(what[3].str().c_str(), nullptr, 10);
        if (day < 1 || day > 31) {
            LOG(WARNING) << "invalid day. [day=" << day << "]";
            return false;
        }

        if (what[4].length()) {
            int hour = strtol(what[5].str().c_str(), nullptr, 10);
            if (hour < 0 || hour > 23) {
                LOG(WARNING) << "invalid hour. [hour=" << hour << "]";
                return false;
            }

            int minute = strtol(what[6].str().c_str(), nullptr, 10);
            if (minute < 0 || minute > 59) {
                LOG(WARNING) << "invalid minute. [minute=" << minute << "]";
                return false;
            }

            int second = strtol(what[7].str().c_str(), nullptr, 10);
            if (second < 0 || second > 59) {
                LOG(WARNING) << "invalid second. [second=" << second << "]";
                return false;
            }
            if (what[8].length()) {
                if (what[9].str().size() > 6) {
                    LOG(WARNING) << "invalid microsecond. [microsecond=" << what[9].str() << "]";
                    return false;
                }
                auto s9 = what[9].str();
                s9.resize(6, '0');
                if (const long ms = strtol(s9.c_str(), nullptr, 10);
                    ms % static_cast<long>(std::pow(10, 6 - scale)) != 0) {
                    LOG(WARNING) << "invalid microsecond. [microsecond=" << what[9].str()
                                 << ", scale = " << scale << "]";
                    return false;
                }
            }
        }

        return true;
    } else {
        LOG(WARNING) << "datetime string does not match";
        return false;
    }
}

bool valid_bool(const std::string& value_str) {
    if (value_str == "0" || value_str == "1") {
        return true;
    }
    StringParser::ParseResult result;
    StringParser::string_to_bool(value_str.c_str(), value_str.length(), &result);
    return result == StringParser::PARSE_SUCCESS;
}

bool valid_ipv4(const std::string& value_str) {
    return IPv4Value::is_valid_string(value_str.c_str(), value_str.size());
}

bool valid_ipv6(const std::string& value_str) {
    return IPv6Value::is_valid_string(value_str.c_str(), value_str.size());
}

} // namespace doris
