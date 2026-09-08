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

#include "storage/index/snii/writer/encoded_spill_run.h"

#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>

#include <algorithm>
#include <array>
#include <bit>
#include <cerrno>
#include <cstring>
#include <limits>
#include <utility>

#include "common/check.h"
#include "storage/index/snii/common/slice.h"
#include "storage/index/snii/encoding/crc32c.h"
#include "storage/index/snii/encoding/varint.h"
#include "storage/index/snii/writer/memory_reporter.h"
#include "storage/index/snii/writer/spill_run_codec.h"

namespace doris::snii::writer {
namespace {

constexpr size_t kRunBlockBytes = 64 * 1024;
constexpr std::array<uint8_t, 8> kRunMagic {'S', 'N', 'I', 'R', 'U', 'N', 2, 0};
constexpr std::array<uint8_t, 8> kRunEnd {'S', 'N', 'I', 'E', 'N', 'D', 2, 0};

Status bad_run(const char* reason) {
    return Status::Error<ErrorCode::INVERTED_INDEX_FILE_CORRUPTED, false>("encoded spill run: {}",
                                                                          reason);
}

Status run_io_error(const char* operation) {
    return Status::Error<ErrorCode::IO_ERROR, false>("encoded spill run {}: {}", operation,
                                                     std::strerror(errno));
}

// Independent forward ranges on one shared fd. The raw compatibility decoder
// can read docids/frequencies/positions without loading any complete section.
class RunFileCursor {
public:
    explicit RunFileCursor(MemoryReporter* reporter)
            : reporter_(reporter),
              reservation_(reporter == nullptr ? MemoryReporter::Reservation()
                                               : reporter->make_postings_reservation()) {}

    Status reset(int fd, uint64_t offset, uint64_t length, uint64_t file_size) {
        if (offset > file_size || length > file_size - offset) {
            return bad_run("section exceeds file size");
        }
        fd_ = fd;
        offset_ = offset;
        end_ = offset + length;
        pos_ = 0;
        available_ = 0;
        return Status::OK();
    }

    uint64_t offset() const { return offset_; }
    uint64_t remaining() const { return end_ - offset_; }

    Status read(std::span<uint8_t> destination) {
        if (destination.size() > remaining()) {
            return bad_run("truncated section");
        }
        while (!destination.empty()) {
            if (pos_ == available_) {
                RETURN_IF_ERROR(fill());
            }
            const size_t count = std::min(destination.size(), available_ - pos_);
            std::memcpy(destination.data(), buffer_.get() + pos_, count);
            pos_ += count;
            offset_ += count;
            destination = destination.subspan(count);
        }
        return Status::OK();
    }

    Status byte(uint8_t* value) {
        if (pos_ == available_) {
            RETURN_IF_ERROR(fill());
        }
        *value = buffer_[pos_++];
        ++offset_;
        return Status::OK();
    }

    Status varint(uint64_t* value) {
        *value = 0;
        for (unsigned shift = 0; shift < 64; shift += 7) {
            uint8_t part = 0;
            RETURN_IF_ERROR(byte(&part));
            if (shift == 63 && part > 1) {
                return bad_run("varint overflows uint64");
            }
            *value |= static_cast<uint64_t>(part & 0x7f) << shift;
            if ((part & 0x80) == 0) {
                return Status::OK();
            }
        }
        return bad_run("unterminated varint");
    }

    Status u32(uint32_t* value) {
        static_assert(std::endian::native == std::endian::little);
        return read({reinterpret_cast<uint8_t*>(value), sizeof(*value)});
    }

    Status checked_header(std::span<uint64_t> values) {
        std::array<uint8_t, 40> bytes {};
        DORIS_CHECK(values.size() <= 4);
        size_t size = 0;
        for (auto& value : values) {
            RETURN_IF_ERROR(varint(&value));
            size += encode_varint64(value, bytes.data() + size);
        }
        uint32_t checksum = 0;
        RETURN_IF_ERROR(u32(&checksum));
        if (checksum != crc32c(Slice(bytes.data(), size))) {
            return bad_run("header CRC mismatch");
        }
        return Status::OK();
    }

    Status skip(uint64_t count) {
        if (count > remaining()) {
            return bad_run("truncated section");
        }
        if (count <= available_ - pos_) {
            pos_ += static_cast<size_t>(count);
        } else {
            pos_ = 0;
            available_ = 0;
        }
        offset_ += count;
        return Status::OK();
    }

private:
    Status fill() {
        if (remaining() == 0) {
            return bad_run("unexpected end of file");
        }
        if (buffer_ == nullptr) {
            if (reporter_ != nullptr) {
                RETURN_IF_ERROR(reservation_.set_bytes(kRunBlockBytes));
            }
            buffer_ = std::make_unique<uint8_t[]>(kRunBlockBytes);
        }
        const size_t want = static_cast<size_t>(std::min<uint64_t>(remaining(), kRunBlockBytes));
        ssize_t count = 0;
        do {
            count = ::pread(fd_, buffer_.get(), want, static_cast<off_t>(offset_));
        } while (count < 0 && errno == EINTR);
        if (count < 0) {
            return run_io_error("read");
        }
        if (count == 0) {
            return bad_run("truncated file");
        }
        if (reporter_ != nullptr) {
            reporter_->record_postings_io(static_cast<uint64_t>(count), 0);
        }
        pos_ = 0;
        available_ = static_cast<size_t>(count);
        return Status::OK();
    }

    MemoryReporter* reporter_;
    MemoryReporter::Reservation reservation_;
    std::unique_ptr<uint8_t[]> buffer_;
    int fd_ = -1;
    uint64_t offset_ = 0;
    uint64_t end_ = 0;
    size_t pos_ = 0;
    size_t available_ = 0;
};

} // namespace

class EncodedRunWriter::Impl {
public:
    explicit Impl(MemoryReporter* reporter)
            : out(reporter, kRunBlockBytes),
              reporter(reporter),
              reservation(reporter == nullptr ? MemoryReporter::Reservation()
                                              : reporter->make_postings_reservation()) {}

    Status append_header(std::span<const uint64_t> values) {
        std::array<uint8_t, 40> bytes {};
        DORIS_CHECK(values.size() <= 4);
        size_t size = 0;
        for (uint64_t value : values) {
            size += encode_varint64(value, bytes.data() + size);
        }
        const uint32_t checksum = crc32c(Slice(bytes.data(), size));
        RETURN_IF_ERROR(out.append_bytes(bytes.data(), size));
        return out.append_raw_u32(&checksum, 1);
    }

    Status flush_payload() {
        if (payload_size == 0) {
            return Status::OK();
        }
        RETURN_IF_ERROR(out.append_varint(payload_size));
        RETURN_IF_ERROR(out.append_bytes(payload.get(), payload_size));
        const uint32_t crc = crc32c(Slice(payload.get(), payload_size));
        RETURN_IF_ERROR(out.append_raw_u32(&crc, 1));
        payload_size = 0;
        return Status::OK();
    }

    Status payload_varint(uint64_t value) {
        std::array<uint8_t, 10> encoded;
        size_t count = 0;
        while (value >= 0x80) {
            encoded[count++] = static_cast<uint8_t>(value) | 0x80;
            value >>= 7;
        }
        encoded[count++] = static_cast<uint8_t>(value);
        return append_payload({encoded.data(), count});
    }

    Status append_payload(std::span<const uint8_t> bytes) {
        DORIS_CHECK(fragment_open);
        if (payload == nullptr && !bytes.empty()) {
            if (reporter != nullptr) {
                RETURN_IF_ERROR(reservation.set_bytes(kRunBlockBytes));
            }
            payload = std::make_unique<uint8_t[]>(kRunBlockBytes);
        }
        while (!bytes.empty()) {
            if (payload_size == kRunBlockBytes) {
                RETURN_IF_ERROR(flush_payload());
            }
            const size_t count = std::min(bytes.size(), kRunBlockBytes - payload_size);
            std::memcpy(payload.get() + payload_size, bytes.data(), count);
            payload_size += count;
            bytes = bytes.subspan(count);
        }
        return Status::OK();
    }

    RunWriter out;
    MemoryReporter* reporter;
    MemoryReporter::Reservation reservation;
    std::unique_ptr<uint8_t[]> payload;
    size_t payload_size = 0;
    bool term_open = false;
    bool fragment_open = false;
    bool run_open = false;
    bool positions = false;
    uint32_t previous_doc = 0;
};

EncodedRunWriter::EncodedRunWriter(MemoryReporter* reporter)
        : reporter_(reporter),
          metadata_(reporter == nullptr ? MemoryReporter::Reservation()
                                        : reporter->make_postings_reservation()) {}
EncodedRunWriter::~EncodedRunWriter() = default;

Status EncodedRunWriter::open(const std::string& path, bool append) {
    if (impl_ == nullptr) {
        if (reporter_ != nullptr) {
            RETURN_IF_ERROR(metadata_.set_bytes(sizeof(Impl)));
        }
        impl_ = std::make_unique<Impl>(reporter_);
    }
    RETURN_IF_ERROR(impl_->out.open(path, append));
    const Status status = impl_->out.append_bytes(kRunMagic.data(), kRunMagic.size());
    if (!status.ok()) {
        // The caller only takes ownership after open succeeds.
        if (!append) {
            std::remove(path.c_str());
        }
        return status;
    }
    impl_->run_open = true;
    return Status::OK();
}

uint64_t EncodedRunWriter::file_offset() const {
    return impl_->out.file_bytes_ + impl_->out.buf_.size();
}

Status EncodedRunWriter::begin_term(const EncodedRunTerm& term) {
    DORIS_CHECK(!impl_->term_open);
    const std::array<uint64_t, 4> header {term.term_id, term.has_positions ? 2U : 1U,
                                          term.document_groups, term.tokens};
    RETURN_IF_ERROR(impl_->append_header(header));
    impl_->positions = term.has_positions;
    impl_->term_open = true;
    return Status::OK();
}

Status EncodedRunWriter::begin_fragment(uint64_t document_groups, uint64_t tokens) {
    DORIS_CHECK(impl_->term_open && !impl_->fragment_open);
    RETURN_IF_ERROR(impl_->out.append_varint(1));
    const std::array<uint64_t, 2> header {document_groups, tokens};
    RETURN_IF_ERROR(impl_->append_header(header));
    impl_->fragment_open = true;
    impl_->previous_doc = 0;
    return Status::OK();
}

Status EncodedRunWriter::append_payload(std::span<const uint8_t> bytes) {
    return impl_->append_payload(bytes);
}

Status EncodedRunWriter::append_token(uint32_t docid, uint32_t position, bool new_document) {
    const uint64_t tagged = (impl_->positions ? static_cast<uint64_t>(position) << 1 : 0) |
                            static_cast<uint64_t>(new_document);
    RETURN_IF_ERROR(impl_->payload_varint(tagged));
    if (new_document) {
        const int64_t delta = static_cast<int64_t>(docid) - impl_->previous_doc;
        const uint64_t encoded =
                (static_cast<uint64_t>(delta) << 1) ^ static_cast<uint64_t>(delta >> 63);
        RETURN_IF_ERROR(impl_->payload_varint(encoded));
        impl_->previous_doc = docid;
    }
    return Status::OK();
}

Status EncodedRunWriter::end_fragment() {
    DORIS_CHECK(impl_->fragment_open);
    RETURN_IF_ERROR(impl_->flush_payload());
    RETURN_IF_ERROR(impl_->out.append_varint(0));
    impl_->fragment_open = false;
    return Status::OK();
}

Status EncodedRunWriter::end_term() {
    DORIS_CHECK(impl_->term_open && !impl_->fragment_open);
    RETURN_IF_ERROR(impl_->out.append_varint(0));
    impl_->term_open = false;
    return Status::OK();
}

Status EncodedRunWriter::write_term(uint32_t term_id, const TermPostings& postings) {
    DORIS_CHECK(postings.docids.size() == postings.freqs.size());
    uint64_t tokens = 0;
    for (uint32_t frequency : postings.freqs) {
        tokens += frequency;
    }
    DORIS_CHECK(!postings.retain_positions || tokens == postings.positions_flat.size());
    RETURN_IF_ERROR(
            begin_term({term_id, postings.retain_positions, postings.docids.size(), tokens}));
    if (postings.docids.empty()) {
        return end_term();
    }
    RETURN_IF_ERROR(begin_fragment(postings.docids.size(), tokens));
    size_t position = 0;
    for (size_t doc = 0; doc < postings.docids.size(); ++doc) {
        for (uint32_t occurrence = 0; occurrence < postings.freqs[doc]; ++occurrence) {
            RETURN_IF_ERROR(
                    append_token(postings.docids[doc],
                                 postings.retain_positions ? postings.positions_flat[position] : 0,
                                 occurrence == 0));
            ++position;
        }
    }
    RETURN_IF_ERROR(end_fragment());
    return end_term();
}

Status EncodedRunWriter::close() {
    if (impl_ == nullptr || !impl_->run_open) {
        return Status::OK();
    }
    DORIS_CHECK(!impl_->term_open && !impl_->fragment_open);
    RETURN_IF_ERROR(impl_->out.append_bytes(kRunEnd.data(), kRunEnd.size()));
    RETURN_IF_ERROR(impl_->out.close());
    impl_->run_open = false;
    impl_->payload.reset();
    impl_->reservation.reset();
    return Status::OK();
}

class StreamingRunReader::Impl {
public:
    explicit Impl(MemoryReporter* reporter)
            : reporter(reporter),
              file(reporter),
              docs(reporter),
              frequencies(reporter),
              positions(reporter),
              payload_reservation(reporter == nullptr ? MemoryReporter::Reservation()
                                                      : reporter->make_postings_reservation()) {}
    ~Impl() {
        if (fd >= 0) {
            ::close(fd);
        }
    }

    Status open(const std::string& path, bool allow_positions, bool allow_legacy, uint64_t begin,
                uint64_t end) {
        fd = ::open(path.c_str(), O_RDONLY | O_CLOEXEC);
        if (fd < 0) {
            return run_io_error("open");
        }
        struct stat statbuf {};
        if (::fstat(fd, &statbuf) != 0 || statbuf.st_size < 0) {
            return run_io_error("stat");
        }
        const auto physical_size = static_cast<uint64_t>(statbuf.st_size);
        file_size = end == UINT64_MAX ? physical_size : end;
        if (begin > file_size || file_size > physical_size) {
            return bad_run("run range exceeds spool size");
        }
        const uint64_t length = file_size - begin;
        has_positions = allow_positions;
        RETURN_IF_ERROR(
                file.reset(fd, begin, std::min<uint64_t>(kRunMagic.size(), length), file_size));
        if (length >= kRunMagic.size()) {
            std::array<uint8_t, kRunMagic.size()> magic;
            RETURN_IF_ERROR(file.read(magic));
            encoded = magic == kRunMagic;
        }
        if (!encoded && !allow_legacy) {
            return bad_run("missing encoded run header");
        }
        if (encoded) {
            if (length < kRunMagic.size() + kRunEnd.size()) {
                return bad_run("run is not sealed");
            }
            std::array<uint8_t, kRunEnd.size()> seal;
            RETURN_IF_ERROR(file.reset(fd, file_size - seal.size(), seal.size(), file_size));
            RETURN_IF_ERROR(file.read(seal));
            if (seal != kRunEnd) {
                return bad_run("run is not sealed");
            }
            file_size -= seal.size();
        }
        next_record = begin + (encoded ? kRunMagic.size() : 0);
        return advance();
    }

    Status advance() {
        if (!term_done) {
            // Explicit advance may skip a partly consumed term. Decode while
            // skipping so CRCs and length/count invariants remain checked.
            bool end = false;
            uint32_t doc = 0;
            uint32_t pos = 0;
            while (!end) {
                RETURN_IF_ERROR(next_token(&doc, &pos, &end));
            }
        }
        if (next_record == file_size) {
            done = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(file.reset(fd, next_record, file_size - next_record, file_size));
        uint64_t id = 0;
        uint64_t shape = 0;
        if (encoded) {
            std::array<uint64_t, 4> header {};
            RETURN_IF_ERROR(file.checked_header(header));
            id = header[0];
            shape = header[1];
            term.document_groups = header[2];
            term.tokens = header[3];
        } else {
            RETURN_IF_ERROR(file.varint(&id));
            RETURN_IF_ERROR(file.varint(&shape));
            RETURN_IF_ERROR(file.varint(&term.document_groups));
            term.tokens = 0;
        }
        if (id > UINT32_MAX || shape > 2 || (encoded && shape == 0)) {
            return bad_run("invalid term id or posting shape");
        }
        if (shape == 2 && !has_positions) {
            return bad_run("positioned record in docs-only run");
        }
        term.term_id = static_cast<uint32_t>(id);
        term.has_positions = shape == 2;
        raw_statless = !encoded && shape == 0;
        groups_seen = 0;
        tokens_seen = 0;
        fragment_remaining = 0;
        fragment_open = false;
        payload_size = 0;
        payload_pos = 0;
        raw_doc = 0;
        raw_frequency_remaining = 0;
        if (!encoded) {
            RETURN_IF_ERROR(open_raw_sections());
        }
        term_done = false;
        done = false;
        return Status::OK();
    }

    Status open_raw_frequencies(uint64_t count, uint64_t length) {
        if (raw_statless) {
            term.tokens = count;
            return Status::OK();
        }
        const uint64_t frequency_offset = file.offset();
        RETURN_IF_ERROR(frequencies.reset(fd, frequency_offset, length, file_size));
        RETURN_IF_ERROR(file.skip(length));
        for (uint64_t i = 0; i < count; ++i) {
            uint32_t frequency = 0;
            RETURN_IF_ERROR(frequencies.u32(&frequency));
            if (frequency == 0 || frequency > UINT64_MAX - term.tokens) {
                return bad_run("invalid frequency sum");
            }
            term.tokens += frequency;
        }
        return frequencies.reset(fd, frequency_offset, length, file_size);
    }

    Status open_raw_sections() {
        const uint64_t count = term.document_groups;
        if (count > file_size / sizeof(uint32_t)) {
            return bad_run("document count exceeds file size");
        }
        const uint64_t length = count * sizeof(uint32_t);
        const uint64_t doc_offset = file.offset();
        RETURN_IF_ERROR(docs.reset(fd, doc_offset, length, file_size));
        RETURN_IF_ERROR(file.skip(length));
        RETURN_IF_ERROR(open_raw_frequencies(count, length));
        if (term.has_positions) {
            uint64_t position_count = 0;
            RETURN_IF_ERROR(file.varint(&position_count));
            if (position_count != term.tokens || position_count > file_size / sizeof(uint32_t)) {
                return bad_run("position count differs from frequencies or exceeds file size");
            }
            const uint64_t position_length = position_count * sizeof(uint32_t);
            RETURN_IF_ERROR(positions.reset(fd, file.offset(), position_length, file_size));
            RETURN_IF_ERROR(file.skip(position_length));
        }
        next_record = file.offset();
        return Status::OK();
    }

    Status next_raw_token(uint32_t* docid, uint32_t* position, bool* end) {
        if (raw_frequency_remaining == 0) {
            if (raw_doc == term.document_groups) {
                term_done = true;
                *end = true;
                return Status::OK();
            }
            uint32_t next_doc = 0;
            RETURN_IF_ERROR(docs.u32(&next_doc));
            if (raw_doc != 0 && next_doc <= current_doc) {
                return bad_run("docids must be strictly ascending within one record");
            }
            current_doc = next_doc;
            raw_frequency_remaining = 1;
            if (!raw_statless) {
                RETURN_IF_ERROR(frequencies.u32(&raw_frequency_remaining));
            }
            ++raw_doc;
        }
        *docid = current_doc;
        *position = 0;
        if (term.has_positions) {
            RETURN_IF_ERROR(positions.u32(position));
        }
        --raw_frequency_remaining;
        *end = false;
        return Status::OK();
    }

    Status load_payload() {
        uint64_t length = 0;
        RETURN_IF_ERROR(file.varint(&length));
        return load_payload(length);
    }

    Status load_payload(uint64_t length) {
        if (length == 0 || length > kRunBlockBytes) {
            return bad_run("unexpected payload end or oversized block");
        }
        if (payload == nullptr) {
            if (reporter != nullptr) {
                RETURN_IF_ERROR(payload_reservation.set_bytes(kRunBlockBytes));
            }
            payload = std::make_unique<uint8_t[]>(kRunBlockBytes);
        }
        payload_size = static_cast<size_t>(length);
        payload_pos = 0;
        RETURN_IF_ERROR(file.read({payload.get(), payload_size}));
        uint32_t stored = 0;
        RETURN_IF_ERROR(file.u32(&stored));
        if (stored != crc32c(Slice(payload.get(), payload_size))) {
            return bad_run("payload CRC mismatch");
        }
        return Status::OK();
    }

    Status payload_varint(uint64_t* value) {
        *value = 0;
        for (unsigned shift = 0; shift < 64; shift += 7) {
            if (payload_pos == payload_size) {
                RETURN_IF_ERROR(load_payload());
            }
            const uint8_t byte = payload[payload_pos++];
            if (shift == 63 && byte > 1) {
                return bad_run("payload varint overflows uint64");
            }
            *value |= static_cast<uint64_t>(byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) {
                return Status::OK();
            }
        }
        return bad_run("unterminated payload varint");
    }

    Status finish_fragment() {
        if (!fragment_open) {
            return Status::OK();
        }
        if (payload_pos != payload_size || fragment_groups_seen != fragment_groups) {
            return bad_run("fragment payload or document count differs from its header");
        }
        uint64_t terminator = 0;
        RETURN_IF_ERROR(file.varint(&terminator));
        if (terminator != 0) {
            return bad_run("extra payload after the declared token count");
        }
        fragment_open = false;
        return Status::OK();
    }

    Status begin_next_fragment(bool* end) {
        RETURN_IF_ERROR(finish_fragment());
        uint64_t marker = 0;
        RETURN_IF_ERROR(file.varint(&marker));
        if (marker == 0) {
            if (groups_seen != term.document_groups || tokens_seen != term.tokens) {
                return bad_run("term counts differ from its fragments");
            }
            term_done = true;
            next_record = file.offset();
            *end = true;
            return Status::OK();
        }
        if (marker != 1) {
            return bad_run("unknown fragment marker");
        }
        std::array<uint64_t, 2> header {};
        RETURN_IF_ERROR(file.checked_header(header));
        fragment_groups = header[0];
        fragment_remaining = header[1];
        if (fragment_groups == 0 || fragment_remaining == 0 ||
            fragment_groups > term.document_groups - groups_seen ||
            fragment_remaining > term.tokens - tokens_seen) {
            return bad_run("fragment counts exceed term counts");
        }
        groups_seen += fragment_groups;
        tokens_seen += fragment_remaining;
        fragment_groups_seen = 0;
        current_doc = 0;
        fragment_open = true;
        payload_pos = 0;
        payload_size = 0;
        *end = false;
        return Status::OK();
    }

    Status next_token(uint32_t* docid, uint32_t* position, bool* end) {
        if (term_done) {
            *end = true;
            return Status::OK();
        }
        if (!encoded) {
            return next_raw_token(docid, position, end);
        }
        if (fragment_remaining == 0) {
            RETURN_IF_ERROR(begin_next_fragment(end));
            if (*end) {
                return Status::OK();
            }
        }
        uint64_t tagged = 0;
        RETURN_IF_ERROR(payload_varint(&tagged));
        if ((tagged >> 1) > UINT32_MAX || (!term.has_positions && (tagged >> 1) != 0)) {
            return bad_run("invalid tagged position");
        }
        if ((tagged & 1) != 0) {
            uint64_t encoded_delta = 0;
            RETURN_IF_ERROR(payload_varint(&encoded_delta));
            const int64_t delta = static_cast<int64_t>(encoded_delta >> 1) ^
                                  -static_cast<int64_t>(encoded_delta & 1);
            if (delta < 0 || std::cmp_greater(delta, UINT32_MAX - current_doc) ||
                (fragment_groups_seen != 0 && delta == 0)) {
                return bad_run("docids must be strictly ascending within a fragment");
            }
            current_doc += static_cast<uint32_t>(delta);
            ++fragment_groups_seen;
        } else if (fragment_groups_seen == 0) {
            return bad_run("fragment starts with a continued document");
        }
        *docid = current_doc;
        *position = static_cast<uint32_t>(tagged >> 1);
        --fragment_remaining;
        *end = false;
        return Status::OK();
    }

    Status copy_raw_fragment(EncodedRunWriter* writer) {
        if (term.document_groups == 0) {
            term_done = true;
            return Status::OK();
        }
        RETURN_IF_ERROR(writer->begin_fragment(term.document_groups, term.tokens));
        bool end = false;
        bool first = true;
        uint32_t previous = 0;
        while (true) {
            uint32_t doc = 0;
            uint32_t position = 0;
            RETURN_IF_ERROR(next_raw_token(&doc, &position, &end));
            if (end) {
                break;
            }
            RETURN_IF_ERROR(writer->append_token(doc, position, first || doc != previous));
            previous = doc;
            first = false;
        }
        return writer->end_fragment();
    }

    Status copy_encoded_fragment(EncodedRunWriter* writer) {
        std::array<uint64_t, 2> header {};
        RETURN_IF_ERROR(file.checked_header(header));
        const uint64_t groups = header[0];
        const uint64_t tokens = header[1];
        if (groups == 0 || tokens == 0 || groups > term.document_groups - groups_seen ||
            tokens > term.tokens - tokens_seen) {
            return bad_run("invalid fragment counts");
        }
        groups_seen += groups;
        tokens_seen += tokens;
        RETURN_IF_ERROR(writer->begin_fragment(groups, tokens));
        while (true) {
            uint64_t length = 0;
            RETURN_IF_ERROR(file.varint(&length));
            if (length == 0) {
                break;
            }
            RETURN_IF_ERROR(load_payload(length));
            RETURN_IF_ERROR(writer->append_payload({payload.get(), payload_size}));
        }
        return writer->end_fragment();
    }

    Status copy_fragments_to(EncodedRunWriter* writer) {
        DORIS_CHECK(!term_done && groups_seen == 0 && raw_doc == 0);
        if (!encoded) {
            return copy_raw_fragment(writer);
        }
        while (true) {
            uint64_t marker = 0;
            RETURN_IF_ERROR(file.varint(&marker));
            if (marker == 0) {
                if (groups_seen != term.document_groups || tokens_seen != term.tokens) {
                    return bad_run("term counts differ from its fragments");
                }
                term_done = true;
                next_record = file.offset();
                return Status::OK();
            }
            if (marker != 1) {
                return bad_run("invalid fragment marker");
            }
            RETURN_IF_ERROR(copy_encoded_fragment(writer));
        }
    }

    MemoryReporter* reporter;
    RunFileCursor file;
    RunFileCursor docs;
    RunFileCursor frequencies;
    RunFileCursor positions;
    MemoryReporter::Reservation payload_reservation;
    std::unique_ptr<uint8_t[]> payload;
    int fd = -1;
    uint64_t file_size = 0;
    uint64_t next_record = 0;
    EncodedRunTerm term;
    bool encoded = false;
    bool raw_statless = false;
    bool has_positions = false;
    bool done = false;
    bool term_done = true;
    bool fragment_open = false;
    size_t payload_pos = 0;
    size_t payload_size = 0;
    uint64_t groups_seen = 0;
    uint64_t tokens_seen = 0;
    uint64_t fragment_groups = 0;
    uint64_t fragment_groups_seen = 0;
    uint64_t fragment_remaining = 0;
    uint64_t raw_doc = 0;
    uint32_t raw_frequency_remaining = 0;
    uint32_t current_doc = 0;
};

StreamingRunReader::StreamingRunReader(MemoryReporter* reporter)
        : reporter_(reporter),
          metadata_(reporter == nullptr ? MemoryReporter::Reservation()
                                        : reporter->make_postings_reservation()) {}
StreamingRunReader::~StreamingRunReader() = default;
size_t StreamingRunReader::object_bytes() {
    return sizeof(StreamingRunReader);
}
Status StreamingRunReader::open(const std::string& path, bool has_positions, bool allow_legacy,
                                uint64_t begin, uint64_t end) {
    if (impl_ == nullptr) {
        if (reporter_ != nullptr) {
            RETURN_IF_ERROR(metadata_.set_bytes(sizeof(Impl)));
        }
        impl_ = std::make_unique<Impl>(reporter_);
    }
    return impl_->open(path, has_positions, allow_legacy, begin, end);
}
Status StreamingRunReader::advance() {
    return impl_->advance();
}
const EncodedRunTerm& StreamingRunReader::current() const {
    return impl_->term;
}
bool StreamingRunReader::exhausted() const {
    return impl_->done;
}
bool StreamingRunReader::term_exhausted() const {
    return impl_->term_done;
}
Status StreamingRunReader::next_token(uint32_t* docid, uint32_t* position, bool* end) {
    return impl_->next_token(docid, position, end);
}
Status StreamingRunReader::copy_fragments_to(EncodedRunWriter* writer) {
    return impl_->copy_fragments_to(writer);
}

} // namespace doris::snii::writer
