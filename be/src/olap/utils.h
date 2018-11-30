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

#ifndef DORIS_BE_SRC_OLAP_UTILS_H
#define DORIS_BE_SRC_OLAP_UTILS_H

#include <fcntl.h>
#include <pthread.h>
#include <sys/time.h>
#include <zlib.h>

#include <cstdio>
#include <cstdlib>
#include <exception>
#include <iterator>
#include <limits>
#include <list>
#include <set>
#include <sstream>
#include <string>
#include <vector>

#include <boost/filesystem.hpp>

#include "common/logging.h"
#include "olap/bhp_lib.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"

namespace doris {
void write_log_info(char* buf, size_t buf_len, const char* fmt, ...);

// 用来加速运算
const static int32_t g_power_table[] = {
    1,
    10,
    100,
    1000,
    10000,
    100000,
    1000000,
    10000000,
    100000000,
    1000000000
};

// 计时工具，用于确定一段代码执行的时间，用于性能调优
class OlapStopWatch {
public:
    uint64_t get_elapse_time_us() {
        struct timeval now;
        gettimeofday(&now, 0);
        return (uint64_t)((now.tv_sec - _begin_time.tv_sec) * 1e6 +
                          (now.tv_usec - _begin_time.tv_usec));
    }

    void reset() {
        gettimeofday(&_begin_time, 0);
    }

    OlapStopWatch() {
        reset();
    }

private:
    struct timeval _begin_time;    // 起始时间戳
};

// 解决notice log buffer不够长的问题, 原生的notice log的buffer只有2048大小
class OLAPNoticeLog {
public:
    static void push(const char* key, const char* fmt, ...) \
    __attribute__((__format__(__printf__, 2, 3)));

    static void log(const char* msg);

private:
    static const int BUF_SIZE = 128 * 1024; // buffer大小
    static __thread char _buf[BUF_SIZE];
    static __thread int _len;
};

// 用于在notice log中输出索引定位次数以及平均定位时间
// 如果还需要在notice log中输出需要聚合计算的其他信息，可以参考这个来实现。
class OLAPNoticeInfo {
public:
    static void add_seek_count();
    static void add_seek_time_us(uint64_t time_us);
    static void add_scan_rows(uint64_t rows);
    static void add_filter_rows(uint64_t rows);
    static uint64_t seek_count();
    static uint64_t seek_time_us();
    static uint64_t avg_seek_time_us();
    static uint64_t scan_rows();
    static uint64_t filter_rows();
    static void clear();

private:
    static __thread uint64_t _seek_count;
    static __thread uint64_t _seek_time_us;
    static __thread uint64_t _scan_rows;
    static __thread uint64_t _filter_rows;
};

#define OLAP_LOG_NOTICE_SOCK(message) OLAPNoticeLog::log(message)
#define OLAP_LOG_NOTICE_PUSH(key, fmt, arg...) OLAPNoticeLog::push(key, fmt, ##arg)

// @brief 切分字符串
// @param base 原串
// @param separator 分隔符
// @param result 切分结果
template <typename T>
OLAPStatus split_string(const std::string& base,
                    const T separator,
                    std::vector<std::string>* result) {
    if (!result) {
        return OLAP_ERR_OTHER_ERROR;
    }

    // 处理base为空的情况
    // 在删除功能中，当varchar类型列的过滤条件为空时，会出现这种情况
    if (base.size() == 0) {
        result->push_back("");
        return OLAP_SUCCESS;
    }

    size_t offset = 0;
    while (offset < base.length()) {
        size_t next = base.find(separator, offset);
        if (next == std::string::npos) {
            result->push_back(base.substr(offset));
            break;
        } else {
            result->push_back(base.substr(offset, next - offset));
            offset = next + 1;
        }
    }

    return OLAP_SUCCESS;
}

template <typename T>
void _destruct_object(const void* obj, void*) {
    delete((const T*)obj);
}

template <typename T>
void _destruct_array(const void* array, void*) {
    delete[]((const T*)array);
}

// 根据压缩类型的不同，执行压缩。dest_buf_len是dest_buf的最大长度，
// 通过指针返回的written_len是实际写入的长度。
OLAPStatus olap_compress(const char* src_buf,
                     size_t src_len,
                     char* dest_buf,
                     size_t dest_len,
                     size_t* written_len,
                     OLAPCompressionType compression_type);

OLAPStatus olap_decompress(const char* src_buf,
                       size_t src_len,
                       char* dest_buf,
                       size_t dest_len,
                       size_t* written_len,
                       OLAPCompressionType compression_type);

// 计算adler32的包装函数
// 第一次使用的时候第一个参数传宏ADLER32_INIT, 之后的调用传上次计算的结果
#define ADLER32_INIT adler32(0L, Z_NULL, 0)
uint32_t olap_adler32(uint32_t adler, const char* buf, size_t len);

// CRC32仅仅用在RowBlock的校验，性能优异
#define CRC32_INIT 0xFFFFFFFF
uint32_t olap_crc32(uint32_t crc32, const char* buf, size_t len);

// 获取系统当前时间，并将时间转换为字符串
OLAPStatus gen_timestamp_string(std::string* out_string);

// 将file移到回收站，回收站位于storage_root/trash, file可以是文件或目录
// 移动的同时将file改名：storage_root/trash/20150619154308/file
OLAPStatus move_to_trash(const boost::filesystem::path& schema_hash_root,
                         const boost::filesystem::path& file_path);

// encapsulation of pthread_mutex to lock the critical sources.
class Mutex {
public:
    Mutex();
    ~Mutex();

    // wait until obtain the lock
    OLAPStatus lock();
    
    // try obtaining the lock
    OLAPStatus trylock();
    
    // unlock is called after lock()
    OLAPStatus unlock();

    pthread_mutex_t* getlock() {
        return &_lock;
    }

private:
    pthread_mutex_t _lock;
    DISALLOW_COPY_AND_ASSIGN(Mutex);
};

// Helper class than locks a mutex on construction
// and unlocks the mutex on descontruction.
class MutexLock {
public:
    // wait until obtain the lock
    explicit MutexLock(Mutex* mutex) : _mutex(mutex) {
        _mutex->lock();
    }
    // unlock is called after
    ~MutexLock() {
        _mutex->unlock();
    }

private:
    Mutex* _mutex;

    DISALLOW_COPY_AND_ASSIGN(MutexLock);
};

// pthread_read/write_lock
class RWMutex {
public:
    // Possible fairness policies for the RWMutex.
    enum class Priority {
        // The lock will prioritize readers at the expense of writers.
        PREFER_READING,

        // The lock will prioritize writers at the expense of readers.
        //
        // Care should be taken when using this fairness policy, as it can lead to
        // unexpected deadlocks (e.g. a writer waiting on the lock will prevent
        // additional readers from acquiring it).
        PREFER_WRITING,
    };

    // Create an RWMutex that prioritized readers by default.
    RWMutex(Priority prio = Priority::PREFER_READING);
    ~RWMutex();
    // wait until obtaining the read lock
    OLAPStatus rdlock();
    // try obtaining the read lock
    OLAPStatus tryrdlock();
    // wait until obtaining the write lock
    OLAPStatus wrlock();
    // try obtaining the write lock
    OLAPStatus trywrlock();
    // unlock
    OLAPStatus unlock();

private:
    pthread_rwlock_t _lock;
};

//
// Acquire a ReadLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
//
class ReadLock {
public:
    explicit ReadLock(RWMutex* mutex)
            : _mutex(mutex) {
        this->_mutex->rdlock();
    }
    ~ReadLock() { this->_mutex->unlock(); }

private:
    RWMutex* _mutex;
    DISALLOW_COPY_AND_ASSIGN(ReadLock);
};

//
// Acquire a WriteLock on the specified RWMutex.
// The Lock will be automatically released then the
// object goes out of scope.
//
class WriteLock {
public:
    explicit WriteLock(RWMutex* mutex)
            : _mutex(mutex) {
        this->_mutex->wrlock();
    }
    ~WriteLock() { this->_mutex->unlock(); }

private:
    RWMutex* _mutex;
    DISALLOW_COPY_AND_ASSIGN(WriteLock);
};

enum ComparatorEnum {
    COMPARATOR_LESS = 0,
    COMPARATOR_LARGER = 1,
};

// encapsulation of pthread_mutex to lock the critical sources.
class Condition {
public:
    explicit Condition(Mutex& mutex);

    ~Condition();

    void wait();

    void wait_for_seconds(uint32_t seconds);

    void notify();

    void notify_all();

private:
    Mutex& _mutex;
    pthread_cond_t _cond;
};

// 处理comparator functor处理过程中出现的错误
class ComparatorException : public std::exception {
public:
    virtual const char* what() const throw() {
        return "exception happens when doing binary search.";
    }
};

// iterator offset，用于二分查找
typedef uint32_t iterator_offset_t;

class BinarySearchIterator : public std::iterator<std::random_access_iterator_tag, size_t> {
public:
    BinarySearchIterator() : _offset(0u) {}
    explicit BinarySearchIterator(iterator_offset_t offset) : _offset(offset) {}
    
    iterator_offset_t operator*() const {
        return _offset;
    }
    
    BinarySearchIterator& operator++() {
        ++_offset;
        return *this;
    }
    
    BinarySearchIterator& operator-=(size_t step) {
        _offset = _offset - step;
        return *this;
    }

    BinarySearchIterator& operator+=(size_t step) {
        _offset = _offset + step;
        return *this;
    }
    
    bool operator!=(const BinarySearchIterator& iterator) {
        return this->_offset != iterator._offset;
    }
    
private:
    iterator_offset_t  _offset;
};

int operator-(const BinarySearchIterator& left, const BinarySearchIterator& right);

// 不用sse4指令的crc32c的计算函数
unsigned int crc32c_lut(char const* b, unsigned int off, unsigned int len, unsigned int crc);

OLAPStatus copy_file(const std::string& src, const std::string& dest);

bool check_dir_existed(const std::string& path);

OLAPStatus create_dir(const std::string& path);
OLAPStatus create_dirs(const std::string& path);

OLAPStatus copy_dir(const std::string &src_dir, const std::string &dst_dir);

void remove_files(const std::vector<std::string>& files);

OLAPStatus remove_dir(const std::string& path);

OLAPStatus remove_parent_dir(const std::string& path);

OLAPStatus remove_all_dir(const std::string& path);

//转换两个list
template<typename T1, typename T2>
void static_cast_assign_vector(std::vector<T1>* v1, const std::vector<T2>& v2) {
    if (NULL != v1) {
        //GCC3.4的模板展开貌似有问题， 这里如果使用迭代器会编译失败
        for (size_t i = 0; i < v2.size(); i++) {
            v1->push_back(static_cast<T1>(v2[i]));
        }
    }
}

// 打印Errno
class Errno {
public:
    // 返回Errno对应的错误信息,线程安全
    static const char* str();
    static const char* str(int no);
    static int no();

private:
    static const int BUF_SIZE = 256;
    static __thread char _buf[BUF_SIZE];
};

OLAPStatus dir_walk(const std::string& root, std::set<std::string>* dirs, std::set<std::string>* files);

inline bool is_io_error(OLAPStatus status) {
    return (((OLAP_ERR_IO_ERROR == status || OLAP_ERR_READ_UNENOUGH == status)&& errno == EIO)
                || OLAP_ERR_CHECKSUM_ERROR == status
                || OLAP_ERR_FILE_DATA_ERROR == status
                || OLAP_ERR_TEST_FILE_ERROR == status
                || OLAP_ERR_ROWBLOCK_READ_INFO_ERROR == status);
}

#define ENDSWITH(str, suffix)   \
    ((str).rfind(suffix) == (str).size() - strlen(suffix))

OLAPStatus remove_unused_files(const std::string& schema_hash_root,
                           const std::set<std::string>& files,
                           const std::string& header,
                           const std::set<std::string>& indices,
                           const std::set<std::string>& datas);

// 检查int8_t, int16_t, int32_t, int64_t的值是否溢出
template <typename T>
bool valid_signed_number(const std::string& value_str) {
    char* endptr = NULL;
    errno = 0;
    int64_t value = strtol(value_str.c_str(), &endptr, 10);

    if ((errno == ERANGE && (value == LONG_MAX || value == LONG_MIN))
            || (errno != 0 && value == 0)
            || endptr == value_str
            || *endptr != '\0') {
        return false;
    }

    if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) {
        return false;
    }

    return true;
}

template <>
bool valid_signed_number<int128_t>(const std::string& value_str);

// 检查uint8_t, uint16_t, uint32_t, uint64_t的值是否溢出
template <typename T>
bool valid_unsigned_number(const std::string& value_str) {
    if (value_str[0] == '-') {
        return false;
    }

    char* endptr = NULL;
    errno = 0;
    uint64_t value = strtoul(value_str.c_str(), &endptr, 10);

    if ((errno == ERANGE && (value == ULONG_MAX))
            || (errno != 0 && value == 0)
            || endptr == value_str
            || *endptr != '\0') {
        return false;
    }

    if (value < std::numeric_limits<T>::min() || value > std::numeric_limits<T>::max()) {
        return false;
    }

    return true;
}

bool valid_decimal(const std::string& value_str, const uint32_t precision, const uint32_t frac);

// 粗略检查date或者datetime类型是否正确
bool valid_datetime(const std::string& value_str);

#define OLAP_LOG_WRITE(level, fmt, arg...) \
    do { \
        char buf[10240] = {0}; \
        write_log_info(buf, 10240, fmt, ##arg); \
        LOG(level) << buf; \
    } while (0)

#define OLAP_VLOG_WRITE(level, fmt, arg...) \
    do { \
        if (OLAP_UNLIKELY(VLOG_IS_ON(level))) { \
            char buf[10240] = {0}; \
            write_log_info(buf, 10240, fmt, ##arg); \
            VLOG(level) << buf; \
        } \
    } while (0)

// Log define for non-network usage
// 屏蔽DEBUG和TRACE日志以满足性能测试需求
#define OLAP_LOG_DEBUG(fmt, arg...)  OLAP_VLOG_WRITE(3, fmt, ##arg)
#define OLAP_LOG_TRACE(fmt, arg...)  OLAP_VLOG_WRITE(20, fmt, ##arg)

#define OLAP_LOG_INFO(fmt, arg...) OLAP_LOG_WRITE(INFO, fmt, ##arg)
#define OLAP_LOG_WARNING(fmt, arg...) OLAP_LOG_WRITE(WARNING, fmt, ##arg)
#define OLAP_LOG_FATAL(fmt, arg...) OLAP_LOG_WRITE(ERROR, fmt, ##arg)

#define OLAP_LOG_DEBUG_SOCK(fmt, arg...) OLAP_LOG_WRITE(INFO, fmt, ##arg)
#define OLAP_LOG_TRACE_SOCK(fmt, arg...) OLAP_LOG_WRITE(INFO, fmt, ##arg)
#define OLAP_LOG_NOTICE_DIRECT_SOCK(fmt, arg...) OLAP_LOG_WRITE(INFO, fmt, ##arg)
#define OLAP_LOG_WARNING_SOCK(fmt, arg...) OLAP_LOG_WRITE(WARNING, fmt, ##arg)
#define OLAP_LOG_FATAL_SOCK(fmt, arg...) OLAP_LOG_WRITE(ERROR, fmt, ##arg)
#define OLAP_LOG_SETBASIC(type, fmt, arg...)

// Util used to get string name of thrift enum item
#define EnumToString(enum_type, index, out) \
    do {\
        std::map<int, const char*>::const_iterator it = _##enum_type##_VALUES_TO_NAMES.find(index);\
        if (it == _##enum_type##_VALUES_TO_NAMES.end()) {\
            out = "NULL";\
        } else {\
            out = it->second;\
        }\
    } while (0)

}  // namespace doris

#endif // DORIS_BE_SRC_OLAP_UTILS_H
