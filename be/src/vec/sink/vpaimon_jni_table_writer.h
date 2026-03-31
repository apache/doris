#pragma once

#include <jni.h>
#include <map>
#include <string>
#include <vector>

#include "common/status.h"
#include "vec/exec/format/parquet/arrow_memory_pool.h"
#include "vec/sink/vpaimon_table_writer.h"

namespace doris {
namespace vectorized {

class Block;

class VPaimonJniTableWriter : public VPaimonTableWriter {
public:
    VPaimonJniTableWriter(const TDataSink& t_sink, const VExprContextSPtrs& output_exprs);
    ~VPaimonJniTableWriter() override;

    Status init_properties(ObjectPool* pool);

    Status open(RuntimeState* state, RuntimeProfile* profile) override;

    Status write(Block& block) override;

    Status close(Status status) override;

private:
    Status _get_jni_env(JNIEnv** env);

    Status _check_jni_exception(JNIEnv* env, const std::string& method_name);
    jobject _to_java_options(JNIEnv* env, const std::map<std::string, std::string>& options);
    Status _write_projected_block(Block& block);
    Status _append_to_buffer(const Block& block);
    Status _flush_buffer();

    Status _abort(JNIEnv* env);

    RuntimeState* _state = nullptr;
    RuntimeProfile* _profile = nullptr;

    // JNI References
    jclass _jni_writer_cls = nullptr;
    jobject _jni_writer_obj = nullptr;

    RuntimeProfile::Counter* _written_rows_counter = nullptr;
    RuntimeProfile::Counter* _written_bytes_counter = nullptr;
    RuntimeProfile::Counter* _send_data_timer = nullptr;
    RuntimeProfile::Counter* _project_timer = nullptr;
    RuntimeProfile::Counter* _arrow_convert_timer = nullptr;
    RuntimeProfile::Counter* _file_store_write_timer = nullptr;
    RuntimeProfile::Counter* _open_timer = nullptr;
    RuntimeProfile::Counter* _close_timer = nullptr;
    RuntimeProfile::Counter* _prepare_commit_timer = nullptr;
    RuntimeProfile::Counter* _serialize_commit_messages_timer = nullptr;
    RuntimeProfile::Counter* _commit_payload_bytes_counter = nullptr;
    RuntimeProfile::Counter* _buffer_flush_count = nullptr;

    // Cached Method IDs
    jmethodID _ctor_id = nullptr;
    jmethodID _open_id = nullptr;
    jmethodID _write_id = nullptr;
    jmethodID _prepare_commit_id = nullptr; // 用于获取 CommitMessage
    jmethodID _abort_id = nullptr;
    jmethodID _close_id = nullptr;

    std::unique_ptr<ArrowMemoryPool<>> _arrow_pool;
    std::unique_ptr<Block> _buffer;
    size_t _batch_max_rows = 32768;
    size_t _batch_max_bytes = 4 * 1024 * 1024;
    size_t _buffered_rows = 0;
    size_t _buffered_bytes = 0;
    size_t _row_count = 0;
};

} // namespace vectorized
} // namespace doris
