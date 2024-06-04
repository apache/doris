#ifndef __APPLE__
#if defined(__aarch64__)
#include <sse2neon.h>
#elif defined(__x86_64__)
#include <emmintrin.h>
#include <immintrin.h>
#include <smmintrin.h>
#endif
#include <byteswap.h>
#include <endian.h>
#include <features.h>
#include <linux/perf_event.h>
#include <malloc.h>
#include <sched.h>
#include <sys/prctl.h>
#include <sys/sysinfo.h>
#include <ucontext.h>
#include <unistd.h>
#else
#define _DARWIN_C_SOURCE
#include <netinet/in.h>
#include <sys/_types/_u_int.h>
#include <sys/sysctl.h>
#ifndef _POSIX_C_SOURCE
#include <mach/vm_page_size.h>
#endif
#endif

// CLucene headers
#include <CLucene.h>
#include <CLucene/analysis/LanguageBasedAnalyzer.h>
#include <CLucene/clucene-config.h>
#include <CLucene/debug/error.h>
#include <CLucene/debug/mem.h>
#include <CLucene/index/IndexReader.h>
#include <CLucene/search/IndexSearcher.h>
#include <CLucene/store/Directory.h>
#include <CLucene/store/IndexInput.h>
#include <CLucene/store/IndexOutput.h>
#include <CLucene/store/RAMDirectory.h>
#include <CLucene/util/Misc.h>
#include <CLucene/util/bkd/bkd_reader.h>

// arrow headers
#include <arrow/array/array_base.h>
#include <arrow/array/array_decimal.h>
#include <arrow/array/array_primitive.h>
#include <arrow/array/builder_base.h>
#include <arrow/array/builder_binary.h>
#include <arrow/array/builder_decimal.h>
#include <arrow/array/builder_nested.h>
#include <arrow/array/builder_primitive.h>
#include <arrow/array/data.h>
#include <arrow/buffer.h>
#include <arrow/io/interfaces.h>
#include <arrow/io/memory.h>
#include <arrow/io/type_fwd.h>
#include <arrow/ipc/writer.h>
#include <arrow/pretty_print.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/status.h>
#include <arrow/type.h>
#include <arrow/type_fwd.h>
#include <arrow/util/decimal.h>
#include <arrow/visit_type_inline.h>
#include <arrow/visitor.h>

// aws headers
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSAuthSigner.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/utils/Array.h>
#include <aws/core/utils/HashingUtils.h>
#include <aws/core/utils/Outcome.h>
#include <aws/core/utils/logging/LogLevel.h>
#include <aws/core/utils/logging/LogSystemInterface.h>
#include <aws/core/utils/memory/stl/AWSAllocator.h>
#include <aws/core/utils/memory/stl/AWSMap.h>
#include <aws/core/utils/memory/stl/AWSStreamFwd.h>
#include <aws/core/utils/memory/stl/AWSString.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/utils/memory/stl/AWSVector.h>
#include <aws/core/utils/stream/PreallocatedStreamBuf.h>
#include <aws/core/utils/threading/Executor.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/model/AbortMultipartUploadRequest.h>
#include <aws/s3/model/AbortMultipartUploadResult.h>
#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompleteMultipartUploadResult.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CompletedPart.h>
#include <aws/s3/model/CopyObjectRequest.h>
#include <aws/s3/model/CopyObjectResult.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadResult.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectResult.h>
#include <aws/s3/model/DeleteObjectsRequest.h>
#include <aws/s3/model/DeleteObjectsResult.h>
#include <aws/s3/model/Error.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/GetObjectResult.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/HeadObjectResult.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/ListObjectsV2Result.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/PutObjectResult.h>
#include <aws/s3/model/UploadPartRequest.h>
#include <aws/s3/model/UploadPartResult.h>
#include <aws/transfer/TransferHandle.h>
#include <aws/transfer/TransferManager.h>

// bitshuffle headers
#include <bitshuffle/bitshuffle.h>

// brpc headers
#include <brpc/adaptive_connection_type.h>
#include <brpc/adaptive_protocol_type.h>
#include <brpc/callback.h>
#include <brpc/channel.h>
#include <brpc/closure_guard.h>
#include <brpc/controller.h>
#include <brpc/http_header.h>
#include <brpc/http_method.h>
#include <brpc/server.h>
#include <brpc/ssl_options.h>
#include <brpc/uri.h>

// bthread headers
#include <bthread/bthread.h>
#include <bthread/condition_variable.h>
#include <bthread/mutex.h>
#include <bthread/types.h>

// butil headers
#include <butil/endpoint.h>
#include <butil/errno.h>
#include <butil/fast_rand.h>
#include <butil/fd_utility.h>
#include <butil/iobuf.h>
#include <butil/iobuf_inl.h>
#include <butil/macros.h>
#include <butil/time.h>

// bvar headers
#include <bvar/bvar.h>
#include <bvar/latency_recorder.h>
#include <bvar/reducer.h>
#include <bvar/window.h>

// cctz headers
#include <cctz/civil_time.h>
#include <cctz/civil_time_detail.h>
#include <cctz/time_zone.h>

// event2 headers
#include <event2/buffer.h>
#include <event2/event.h>
#include <event2/http.h>
#include <event2/http_struct.h>
#include <event2/keyvalq_struct.h>
#include <event2/thread.h>

// fast_float headers
#include <fast_float/fast_float.h>
#include <fast_float/parse_number.h>

// fmt headers
#include <fmt/core.h>
#include <fmt/format.h>
#include <fmt/ranges.h>

// gen_cpp headers
#include <gen_cpp/AgentService_types.h>
#include <gen_cpp/BackendService.h>
#include <gen_cpp/BackendService_types.h>
#include <gen_cpp/DataSinks_types.h>
#include <gen_cpp/Data_types.h>
#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/DorisExternalService_types.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/FrontendService.h>
#include <gen_cpp/FrontendService_types.h>
#include <gen_cpp/HeartbeatService.h>
#include <gen_cpp/HeartbeatService_constants.h>
#include <gen_cpp/HeartbeatService_types.h>
#include <gen_cpp/MasterService_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/PaloBrokerService_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/Partitions_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Planner_types.h>
#include <gen_cpp/QueryPlanExtra_types.h>
#include <gen_cpp/RuntimeProfile_types.h>
#include <gen_cpp/Status_types.h>
#include <gen_cpp/TPaloBrokerService.h>
#include <gen_cpp/Types_constants.h>
#include <gen_cpp/Types_types.h>
#include <gen_cpp/data.pb.h>
#include <gen_cpp/descriptors.pb.h>
#include <gen_cpp/function_service.pb.h>
#include <gen_cpp/internal_service.pb.h>
#include <gen_cpp/olap_common.pb.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/parquet_types.h>
#include <gen_cpp/segment_v2.pb.h>
#include <gen_cpp/types.pb.h>

// gflags headers
#include <gflags/gflags.h>
#include <gflags/gflags_declare.h>

// glog headers
#include <glog/logging.h>
#include <glog/stl_logging.h>

// protobuf headers
#include <google/protobuf/service.h>
#include <google/protobuf/stubs/callback.h>
#include <google/protobuf/stubs/common.h>
#include <google/protobuf/stubs/port.h>

#if !defined(__SANITIZE_ADDRESS__) && !defined(ADDRESS_SANITIZER) && !defined(LEAK_SANITIZER) && \
        !defined(THREAD_SANITIZER) && !defined(USE_JEMALLOC)
// gperftools headers
#include <gperftools/malloc_extension.h>
#include <gperftools/malloc_hook.h>
#include <gperftools/nallocx.h>
#include <gperftools/tcmalloc.h>
#endif

// hs headers
#include <hs/hs.h>
#include <hs/hs_common.h>
#include <hs/hs_compile.h>
#include <hs/hs_runtime.h>

// jemalloc headers
#include <jemalloc/jemalloc.h>

// json2pb headers
#include <json2pb/json_to_pb.h>
#include <json2pb/pb_to_json.h>
#include <json2pb/zero_copy_stream_reader.h>

// lz4 headers
#include <lz4/lz4.h>
#include <lz4/lz4frame.h>

// mysql headers
#include <mysql/mysql.h>

// openssl headers
#include <openssl/err.h>
#include <openssl/evp.h>
#include <openssl/md5.h>
#include <openssl/ossl_typ.h>

// parallel_hashmap headers
#include <parallel_hashmap/btree.h>
#include <parallel_hashmap/phmap.h>

// parquet headers
#include <parquet/arrow/reader.h>
#include <parquet/column_writer.h>
#include <parquet/exception.h>
#include <parquet/file_reader.h>
#include <parquet/file_writer.h>
#include <parquet/metadata.h>
#include <parquet/platform.h>
#include <parquet/properties.h>
#include <parquet/schema.h>
#include <parquet/type_fwd.h>
#include <parquet/types.h>

// rapidjson headers
#include <rapidjson/allocators.h>
#include <rapidjson/document.h>
#include <rapidjson/encodings.h>
#include <rapidjson/error/en.h>
#include <rapidjson/prettywriter.h>
#include <rapidjson/rapidjson.h>
#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

// re2 headers
#include <re2/re2.h>
#include <re2/stringpiece.h>

// rocksdb headers
#include <rocksdb/iterator.h>
#include <rocksdb/status.h>

// sanitizer headers
#include <sanitizer/asan_interface.h>
#include <sanitizer/lsan_interface.h>

// simdjson headers
#include <simdjson.h>
#include <simdjson/common_defs.h>
#include <simdjson/simdjson.h>

// snappy headers
#include <snappy.h>
#include <snappy/snappy-sinksource.h>
#include <snappy/snappy.h>

// thrift headers
#include <thrift/TApplicationException.h>
#include <thrift/TOutput.h>
#include <thrift/Thrift.h>
#include <thrift/concurrency/ThreadFactory.h>
#include <thrift/concurrency/ThreadManager.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/protocol/TCompactProtocol.h>
#include <thrift/protocol/TDebugProtocol.h>
#include <thrift/protocol/TJSONProtocol.h>
#include <thrift/protocol/TProtocol.h>
#include <thrift/server/TNonblockingServer.h>
#include <thrift/server/TServer.h>
#include <thrift/server/TThreadPoolServer.h>
#include <thrift/server/TThreadedServer.h>
#include <thrift/transport/TBufferTransports.h>
#include <thrift/transport/TNonblockingServerSocket.h>
#include <thrift/transport/TServerSocket.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransport.h>
#include <thrift/transport/TTransportException.h>

// boost headers
#include <boost/algorithm/string.hpp>
#include <boost/algorithm/string/case_conv.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/detail/classification.hpp>
#include <boost/algorithm/string/join.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/predicate_facade.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <boost/container/detail/std_fwd.hpp>
#include <boost/container/pmr/monotonic_buffer_resource.hpp>
#include <boost/container/pmr/vector.hpp>
#include <boost/container_hash/hash.hpp>
#include <boost/core/noncopyable.hpp>
#include <boost/dynamic_bitset.hpp>
#include <boost/intrusive/detail/algo_type.hpp>
#include <boost/intrusive/list.hpp>
#include <boost/intrusive/list_hook.hpp>
#include <boost/iterator/iterator_adaptor.hpp>
#include <boost/iterator/iterator_facade.hpp>
#include <boost/lexical_cast.hpp>
#include <boost/lexical_cast/bad_lexical_cast.hpp>
#include <boost/noncopyable.hpp>
#include <boost/operators.hpp>
#include <boost/preprocessor/repetition/repeat_from_to.hpp>
#include <boost/random/mersenne_twister.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/stacktrace.hpp>
#include <boost/token_functions.hpp>
#include <boost/tokenizer.hpp>
#include <boost/type_index/type_index_facade.hpp>
#include <boost/uuid/random_generator.hpp>
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

// roaring headers
#include <roaring/roaring.hh>

// c/c++ headers
#include <ctype.h>
#include <cxxabi.h>
#include <float.h>
#include <limits.h>
#include <math.h>
#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <stdlib.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <bitset>
#include <cassert>
#include <cctype>
#include <cerrno>
#include <cfloat>
#include <charconv>
#include <chrono>
#include <cinttypes>
#include <climits>
#include <cmath>
#include <complex>
#include <condition_variable>
#include <csignal>
#include <cstdarg>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <ctime>
#include <deque>
#include <exception>
#include <filesystem>
#include <fstream>
#include <functional>
#include <future>
#include <initializer_list>
#include <iomanip>
#include <iosfwd>
#include <iostream>
#include <istream>
#include <iterator>
#include <limits>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <new>
#include <numeric>
#include <optional>
#include <ostream>
#include <queue>
#include <random>
#include <ratio>
#include <regex>
#include <set>
#include <shared_mutex>
#include <sstream>
#include <stack>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <tuple>
#include <type_traits>
#include <typeindex>
#include <typeinfo>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <variant>
#include <vector>

// zlib headers
#include <bzlib.h>
#include <zlib.h>

// curl headers
#include <curl/curl.h>
#include <curl/system.h>

// jdk headers
#include <jni.h>
#include <jni_md.h>

// libdivide header
#include <libdivide.h>

// librdkafka header
#include <librdkafka/rdkafkacpp.h>

// pdqsort header
#include <pdqsort.h>

// usr headers
#include <arpa/inet.h>
#include <assert.h>
#include <dlfcn.h>
#include <errno.h>
#include <fcntl.h>
#include <fenv.h> // also in c++ headers
#include <iconv.h>
#include <inttypes.h>
#include <libgen.h>
#include <netdb.h>
#include <netinet/in.h>
#include <pthread.h>
#include <sched.h>
#include <setjmp.h>
#include <signal.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <sys/mman.h>
#include <sys/param.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/syscall.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/ucontext.h>
#include <sys/uio.h>
#include <sys/un.h>
#include <wchar.h>

// zconf header
#include <zconf.h>

// zstd headers
#include <zstd.h>
#include <zstd_errors.h>

#ifndef USE_LIBCPP
#include <ext/pb_ds/priority_queue.hpp>
#include <memory_resource>
#endif

// doris headers
#include "common/config.h"
#include "common/status.h"
#include "common/version_internal.h"
#include "olap/olap_common.h"
