#include "io/cloud/cached_remote_file_writer.h"

#include <memory>

#include "common/status.h"
#include "io/cloud/cloud_file_cache.h"
#include "io/cloud/cloud_file_cache_factory.h"
#include "io/cloud/cloud_file_segment.h"
#include "io/fs/s3_file_writer.h"

namespace doris {
namespace io {

CachedRemoteFileWriter::CachedRemoteFileWriter(FileWriterPtr remote_file_writer)
        : _remote_file_writer(std::move(remote_file_writer)) {}

Status CachedRemoteFileWriter::open() {
    RETURN_IF_ERROR(_remote_file_writer->open());
    _cache_key = IFileCache::hash(_remote_file_writer->path().filename().native());
    _cache = FileCacheFactory::instance().get_by_path(_cache_key);
    return Status::OK();
}

Status CachedRemoteFileWriter::append(const Slice& data) {
    RETURN_IF_ERROR(_remote_file_writer->append(data));
    if (_need_buffer) {
        _buffer.append(data.data, data.size);
    }
    return Status::OK();
}

Status CachedRemoteFileWriter::appendv(const Slice* data, size_t data_cnt) {
    RETURN_IF_ERROR(_remote_file_writer->appendv(data, data_cnt));
    if (_need_buffer) {
        for (size_t i = 0; i < data_cnt; i++) {
            _buffer.append((data + i)->get_data(), (data + i)->size);
        }
    }
    return Status::OK();
}

Status CachedRemoteFileWriter::write_at(size_t offset, const Slice& data) {
    RETURN_IF_ERROR(_remote_file_writer->write_at(offset, data));
    if (!_need_buffer) return Status::OK();

    // if offset is larger than _buffer_start_offset, it mean all of the modify is included in buffer too.
    // if offset + data.size > _buffer_start_offset, it mean the Part of the modify is included in buffer.
    if (offset >= _buffer_start_offset) {
        _buffer.replace(offset - _buffer_start_offset, data.size, data.data);
    } else if (offset + data.size > _buffer_start_offset) {
        size_t not_included_size = _buffer_start_offset - offset;
        _buffer.replace(0, data.size - not_included_size, data.data + not_included_size);
    }
    return Status::OK();
}

Status CachedRemoteFileWriter::finalize() {
    RETURN_IF_ERROR(_remote_file_writer->finalize());
    return _need_buffer ? put_buffer_to_cache() : Status::OK();
}

Status CachedRemoteFileWriter::put_buffer_to_cache() {
    std::unique_ptr<int, std::function<void(int*)>> defer((int*)0x01,
                                                          [this](int*) { _need_buffer = false; });
    io::FileSegmentsHolder holder =
            _cache->get_or_set(_cache_key, _buffer_start_offset, _buffer.size(), true, TUniqueId());
    for (auto& segment : holder.file_segments) {
        switch (segment->state()) {
        case FileSegment::State::EMPTY: {
            segment->get_or_set_downloader();
            if (segment->is_downloader()) {
                Slice slice(_buffer.data() + (segment->range().left - _buffer_start_offset),
                            segment->range().size());
                RETURN_IF_ERROR(segment->append(slice));
                RETURN_IF_ERROR(segment->finalize_write());
            } else {
                // should not be here
                LOG(WARNING) << fmt::format("The thread isn't downloader state");
            }
            break;
        }
        default:
            // should not be here
            LOG(WARNING) << fmt::format("The segment state is {}",
                                        FileSegment::state_to_string(segment->state()));
        }
    }
    return Status::OK();
}

} // namespace io
} // namespace doris
