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
// This file is copied from
// https://github.com/apache/impala/blob/branch-3.0.0/be/src/runtime/buffered-tuple-stream.cc
// and modified by Doris

#include <gutil/strings/substitute.h>

#include "runtime/buffered_tuple_stream3.inline.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/row_batch.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "runtime/tuple_row.h"
#include "util/bit_util.h"
#include "util/debug_util.h"

#ifdef NDEBUG
#define CHECK_CONSISTENCY_FAST()
#define CHECK_CONSISTENCY_FULL()
#else
#define CHECK_CONSISTENCY_FAST() CheckConsistencyFast()
#define CHECK_CONSISTENCY_FULL() CheckConsistencyFull()
#endif

using namespace doris;
using namespace strings;

using BufferHandle = BufferPool::BufferHandle;

BufferedTupleStream3::BufferedTupleStream3(RuntimeState* state, const RowDescriptor* row_desc,
                                           BufferPool::ClientHandle* buffer_pool_client,
                                           int64_t default_page_len,
                                           const std::set<SlotId>& ext_varlen_slots)
        : state_(state),
          desc_(row_desc),
          node_id_(-1),
          buffer_pool_(state->exec_env()->buffer_pool()),
          buffer_pool_client_(buffer_pool_client),
          num_pages_(0),
          total_byte_size_(0),
          has_read_iterator_(false),
          read_page_reservation_(buffer_pool_client_),
          read_page_rows_returned_(-1),
          read_ptr_(nullptr),
          read_end_ptr_(nullptr),
          write_ptr_(nullptr),
          write_end_ptr_(nullptr),
          rows_returned_(0),
          has_write_iterator_(false),
          write_page_(nullptr),
          write_page_reservation_(buffer_pool_client_),
          bytes_pinned_(0),
          num_rows_(0),
          default_page_len_(default_page_len),
          has_nullable_tuple_(row_desc->is_any_tuple_nullable()),
          delete_on_read_(false),
          closed_(false),
          pinned_(true) {
    DCHECK(BitUtil::IsPowerOf2(default_page_len)) << default_page_len;
    read_page_ = pages_.end();
    for (int i = 0; i < desc_->tuple_descriptors().size(); ++i) {
        const TupleDescriptor* tuple_desc = desc_->tuple_descriptors()[i];
        const int tuple_byte_size = tuple_desc->byte_size();
        fixed_tuple_sizes_.push_back(tuple_byte_size);

        vector<SlotDescriptor*> tuple_string_slots;
        vector<SlotDescriptor*> tuple_coll_slots;
        for (int j = 0; j < tuple_desc->slots().size(); ++j) {
            SlotDescriptor* slot = tuple_desc->slots()[j];
            if (!slot->type().is_var_len_string_type()) continue;
            if (ext_varlen_slots.find(slot->id()) == ext_varlen_slots.end()) {
                if (slot->type().is_var_len_string_type()) {
                    tuple_string_slots.push_back(slot);
                } else {
                    DCHECK(slot->type().is_collection_type());
                    tuple_coll_slots.push_back(slot);
                }
            }
        }
        if (!tuple_string_slots.empty()) {
            inlined_string_slots_.push_back(make_pair(i, tuple_string_slots));
        }
        /*
    if (!tuple_coll_slots.empty()) {
      inlined_coll_slots_.push_back(make_pair(i, tuple_coll_slots));
    }
*/
    }
}

BufferedTupleStream3::~BufferedTupleStream3() {
    DCHECK(closed_);
}

void BufferedTupleStream3::CheckConsistencyFull() const {
    CheckConsistencyFast();
    // The below checks require iterating over all the pages in the stream.
    DCHECK_EQ(bytes_pinned_, CalcBytesPinned()) << DebugString();
    DCHECK_EQ(pages_.size(), num_pages_) << DebugString();
    for (const Page& page : pages_) CheckPageConsistency(&page);
}

void BufferedTupleStream3::CheckConsistencyFast() const {
    // All the below checks should be O(1).
    DCHECK(has_write_iterator() || write_page_ == nullptr);
    if (write_page_ != nullptr) {
        CheckPageConsistency(write_page_);
        DCHECK(write_page_->is_pinned());
        DCHECK(write_page_->retrieved_buffer);
        const BufferHandle* write_buffer;
        Status status = write_page_->GetBuffer(&write_buffer);
        DCHECK(status.ok()); // Write buffer should never have been unpinned.
        DCHECK_GE(write_ptr_, write_buffer->data());
        DCHECK_EQ(write_end_ptr_, write_buffer->data() + write_page_->len());
        DCHECK_GE(write_end_ptr_, write_ptr_);
    }
    DCHECK(has_read_iterator() || read_page_ == pages_.end());
    if (read_page_ != pages_.end()) {
        CheckPageConsistency(&*read_page_);
        DCHECK(read_page_->is_pinned());
        DCHECK(read_page_->retrieved_buffer);
        // Can't check read buffer without affecting behaviour, because a read may be in
        // flight and this would required blocking on that write.
        DCHECK_GE(read_end_ptr_, read_ptr_);
    }
    if (NeedReadReservation()) {
        DCHECK_EQ(default_page_len_, read_page_reservation_.GetReservation()) << DebugString();
    } else if (!read_page_reservation_.is_closed()) {
        DCHECK_EQ(0, read_page_reservation_.GetReservation());
    }
    if (NeedWriteReservation()) {
        DCHECK_EQ(default_page_len_, write_page_reservation_.GetReservation());
    } else if (!write_page_reservation_.is_closed()) {
        DCHECK_EQ(0, write_page_reservation_.GetReservation());
    }
}

void BufferedTupleStream3::CheckPageConsistency(const Page* page) const {
    DCHECK_EQ(ExpectedPinCount(pinned_, page), page->pin_count()) << DebugString();
    // Only one large row per page.
    if (page->len() > default_page_len_) DCHECK_LE(page->num_rows, 1);
    // We only create pages when we have a row to append to them.
    DCHECK_GT(page->num_rows, 0);
}

string BufferedTupleStream3::DebugString() const {
    std::stringstream ss;
    ss << "BufferedTupleStream3 num_rows=" << num_rows_ << " rows_returned=" << rows_returned_
       << " pinned=" << pinned_ << " delete_on_read=" << delete_on_read_ << " closed=" << closed_
       << "\n"
       << " bytes_pinned=" << bytes_pinned_ << " has_write_iterator=" << has_write_iterator_
       << " write_page=" << write_page_ << " has_read_iterator=" << has_read_iterator_
       << " read_page=";
    if (read_page_ == pages_.end()) {
        ss << "<end>";
    } else {
        ss << &*read_page_;
    }
    ss << "\n"
       << " read_page_reservation=";
    if (read_page_reservation_.is_closed()) {
        ss << "<closed>";
    } else {
        ss << read_page_reservation_.GetReservation();
    }
    ss << " write_page_reservation=";
    if (write_page_reservation_.is_closed()) {
        ss << "<closed>";
    } else {
        ss << write_page_reservation_.GetReservation();
    }
    ss << "\n # pages=" << num_pages_ << " pages=[\n";
    for (const Page& page : pages_) {
        ss << "{" << page.DebugString() << "}";
        if (&page != &pages_.back()) ss << ",\n";
    }
    ss << "]";
    return ss.str();
}

string BufferedTupleStream3::Page::DebugString() const {
    //return Substitute("$0 num_rows=$1", handle.DebugString(), num_rows);
    return string("");
}

Status BufferedTupleStream3::Init(int node_id, bool pinned) {
    //  if (!pinned) UnpinStream(UNPIN_ALL_EXCEPT_CURRENT);
    node_id_ = node_id;
    return Status::OK();
}

Status BufferedTupleStream3::PrepareForWrite(bool* got_reservation) {
    // This must be the first iterator created.
    DCHECK(pages_.empty());
    DCHECK(!delete_on_read_);
    DCHECK(!has_write_iterator());
    DCHECK(!has_read_iterator());
    CHECK_CONSISTENCY_FULL();

    *got_reservation = buffer_pool_client_->IncreaseReservationToFit(default_page_len_);
    if (!*got_reservation) return Status::OK();
    has_write_iterator_ = true;
    // Save reservation for the write iterators.
    buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
    CHECK_CONSISTENCY_FULL();
    return Status::OK();
}

Status BufferedTupleStream3::PrepareForReadWrite(bool delete_on_read, bool* got_reservation) {
    // This must be the first iterator created.
    DCHECK(pages_.empty());
    DCHECK(!delete_on_read_);
    DCHECK(!has_write_iterator());
    DCHECK(!has_read_iterator());
    CHECK_CONSISTENCY_FULL();

    *got_reservation = buffer_pool_client_->IncreaseReservationToFit(2 * default_page_len_);
    if (!*got_reservation) return Status::OK();
    has_write_iterator_ = true;
    // Save reservation for both the read and write iterators.
    buffer_pool_client_->SaveReservation(&read_page_reservation_, default_page_len_);
    buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
    RETURN_IF_ERROR(PrepareForReadInternal(delete_on_read));
    return Status::OK();
}

void BufferedTupleStream3::Close(RowBatch* batch, RowBatch::FlushMode flush) {
    for (Page& page : pages_) {
        if (batch != nullptr && page.retrieved_buffer) {
            // Subtle: We only need to attach buffers from pages that we may have returned
            // references to. ExtractBuffer() cannot fail for these pages because the data
            // is guaranteed to already be in -memory.
            BufferPool::BufferHandle buffer;
            Status status = buffer_pool_->ExtractBuffer(buffer_pool_client_, &page.handle, &buffer);
            DCHECK(status.ok());
            batch->add_buffer(buffer_pool_client_, std::move(buffer), flush);
        } else {
            buffer_pool_->DestroyPage(buffer_pool_client_, &page.handle);
        }
    }
    read_page_reservation_.Close();
    write_page_reservation_.Close();
    pages_.clear();
    num_pages_ = 0;
    bytes_pinned_ = 0;
    closed_ = true;
}

int64_t BufferedTupleStream3::CalcBytesPinned() const {
    int64_t result = 0;
    for (const Page& page : pages_) result += page.pin_count() * page.len();
    return result;
}

Status BufferedTupleStream3::PinPage(Page* page) {
    RETURN_IF_ERROR(buffer_pool_->Pin(buffer_pool_client_, &page->handle));
    bytes_pinned_ += page->len();
    return Status::OK();
}

int BufferedTupleStream3::ExpectedPinCount(bool stream_pinned, const Page* page) const {
    return (stream_pinned || is_read_page(page) || is_write_page(page)) ? 1 : 0;
}

Status BufferedTupleStream3::PinPageIfNeeded(Page* page, bool stream_pinned) {
    int new_pin_count = ExpectedPinCount(stream_pinned, page);
    if (new_pin_count != page->pin_count()) {
        DCHECK_EQ(new_pin_count, page->pin_count() + 1);
        RETURN_IF_ERROR(PinPage(page));
    }
    return Status::OK();
}

void BufferedTupleStream3::UnpinPageIfNeeded(Page* page, bool stream_pinned) {
    int new_pin_count = ExpectedPinCount(stream_pinned, page);
    if (new_pin_count != page->pin_count()) {
        DCHECK_EQ(new_pin_count, page->pin_count() - 1);
        buffer_pool_->Unpin(buffer_pool_client_, &page->handle);
        bytes_pinned_ -= page->len();
        if (page->pin_count() == 0) page->retrieved_buffer = false;
    }
}

bool BufferedTupleStream3::NeedWriteReservation() const {
    return NeedWriteReservation(pinned_);
}

bool BufferedTupleStream3::NeedWriteReservation(bool stream_pinned) const {
    return NeedWriteReservation(stream_pinned, num_pages_, has_write_iterator(),
                                write_page_ != nullptr, has_read_write_page());
}

bool BufferedTupleStream3::NeedWriteReservation(bool stream_pinned, int64_t num_pages,
                                                bool has_write_iterator, bool has_write_page,
                                                bool has_read_write_page) {
    if (!has_write_iterator) return false;
    // If the stream is empty the write reservation hasn't been used yet.
    if (num_pages == 0) return true;
    if (stream_pinned) {
        // Make sure we've saved the write reservation for the next page if the only
        // page is a read/write page.
        return has_read_write_page && num_pages == 1;
    } else {
        // Make sure we've saved the write reservation if it's not being used to pin
        // a page in the stream.
        return !has_write_page || has_read_write_page;
    }
}

bool BufferedTupleStream3::NeedReadReservation() const {
    return NeedReadReservation(pinned_);
}

bool BufferedTupleStream3::NeedReadReservation(bool stream_pinned) const {
    return NeedReadReservation(stream_pinned, num_pages_, has_read_iterator(),
                               read_page_ != pages_.end());
}

bool BufferedTupleStream3::NeedReadReservation(bool stream_pinned, int64_t num_pages,
                                               bool has_read_iterator, bool has_read_page) const {
    return NeedReadReservation(stream_pinned, num_pages, has_read_iterator, has_read_page,
                               has_write_iterator(), write_page_ != nullptr);
}

bool BufferedTupleStream3::NeedReadReservation(bool stream_pinned, int64_t num_pages,
                                               bool has_read_iterator, bool has_read_page,
                                               bool has_write_iterator, bool has_write_page) {
    if (!has_read_iterator) return false;
    if (stream_pinned) {
        // Need reservation if there are no pages currently pinned for reading but we may add
        // a page.
        return num_pages == 0 && has_write_iterator;
    } else {
        // Only need to save reservation for an unpinned stream if there is no read page
        // and we may advance to one in the future.
        return (has_write_iterator || num_pages > 0) && !has_read_page;
    }
}

Status BufferedTupleStream3::NewWritePage(int64_t page_len) noexcept {
    DCHECK(!closed_);
    DCHECK(write_page_ == nullptr);

    Page new_page;
    const BufferHandle* write_buffer;
    RETURN_IF_ERROR(buffer_pool_->CreatePage(buffer_pool_client_, page_len, &new_page.handle,
                                             &write_buffer));
    bytes_pinned_ += page_len;
    total_byte_size_ += page_len;

    pages_.push_back(std::move(new_page));
    ++num_pages_;
    write_page_ = &pages_.back();
    DCHECK_EQ(write_page_->num_rows, 0);
    write_ptr_ = write_buffer->data();
    write_end_ptr_ = write_ptr_ + page_len;
    return Status::OK();
}

void BufferedTupleStream3::CalcPageLenForRow(int64_t row_size, int64_t* page_len) {
    *page_len = std::max(default_page_len_, BitUtil::RoundUpToPowerOfTwo(row_size));
}

Status BufferedTupleStream3::AdvanceWritePage(int64_t row_size, bool* got_reservation) noexcept {
    DCHECK(has_write_iterator());
    CHECK_CONSISTENCY_FAST();

    int64_t page_len;

    CalcPageLenForRow(row_size, &page_len);

    // Reservation may have been saved for the next write page, e.g. by PrepareForWrite()
    // if the stream is empty.
    int64_t write_reservation_to_restore = 0, read_reservation_to_restore = 0;
    if (NeedWriteReservation(pinned_, num_pages_, true, write_page_ != nullptr,
                             has_read_write_page()) &&
        !NeedWriteReservation(pinned_, num_pages_ + 1, true, true, false)) {
        write_reservation_to_restore = default_page_len_;
    }
    // If the stream is pinned, we need to keep the previous write page pinned for reading.
    // Check if we saved reservation for this case.
    if (NeedReadReservation(pinned_, num_pages_, has_read_iterator(), read_page_ != pages_.end(),
                            true, write_page_ != nullptr) &&
        !NeedReadReservation(pinned_, num_pages_ + 1, has_read_iterator(),
                             read_page_ != pages_.end(), true, true)) {
        read_reservation_to_restore = default_page_len_;
    }

    // We may reclaim reservation by unpinning a page that was pinned for writing.
    int64_t write_page_reservation_to_reclaim =
            (write_page_ != nullptr && !pinned_ && !has_read_write_page()) ? write_page_->len() : 0;
    // Check to see if we can get the reservation before changing the state of the stream.
    if (!buffer_pool_client_->IncreaseReservationToFit(page_len - write_reservation_to_restore -
                                                       read_reservation_to_restore -
                                                       write_page_reservation_to_reclaim)) {
        DCHECK(pinned_ || page_len > default_page_len_)
                << "If the stream is unpinned, this should only fail for large pages";
        CHECK_CONSISTENCY_FAST();
        *got_reservation = false;
        return Status::OK();
    }
    if (write_reservation_to_restore > 0) {
        buffer_pool_client_->RestoreReservation(&write_page_reservation_,
                                                write_reservation_to_restore);
    }
    if (read_reservation_to_restore > 0) {
        buffer_pool_client_->RestoreReservation(&read_page_reservation_,
                                                read_reservation_to_restore);
    }
    ResetWritePage();
    //RETURN_IF_ERROR(NewWritePage(page_len));
    Status status = NewWritePage(page_len);
    if (UNLIKELY(!status.ok())) {
        return status;
    }
    *got_reservation = true;
    return Status::OK();
}

void BufferedTupleStream3::ResetWritePage() {
    if (write_page_ == nullptr) return;
    // Unpin the write page if we're reading in unpinned mode.
    Page* prev_write_page = write_page_;
    write_page_ = nullptr;
    write_ptr_ = nullptr;
    write_end_ptr_ = nullptr;

    // May need to decrement pin count now that it's not the write page, depending on
    // the stream's mode.
    UnpinPageIfNeeded(prev_write_page, pinned_);
}

void BufferedTupleStream3::InvalidateWriteIterator() {
    if (!has_write_iterator()) return;
    ResetWritePage();
    has_write_iterator_ = false;
    // No more pages will be appended to stream - do not need any write reservation.
    write_page_reservation_.Close();
    // May not need a read reservation once the write iterator is invalidated.
    if (NeedReadReservation(pinned_, num_pages_, has_read_iterator(), read_page_ != pages_.end(),
                            true, write_page_ != nullptr) &&
        !NeedReadReservation(pinned_, num_pages_, has_read_iterator(), read_page_ != pages_.end(),
                             false, false)) {
        buffer_pool_client_->RestoreReservation(&read_page_reservation_, default_page_len_);
    }
}

Status BufferedTupleStream3::NextReadPage() {
    DCHECK(has_read_iterator());
    DCHECK(!closed_);
    CHECK_CONSISTENCY_FAST();

    if (read_page_ == pages_.end()) {
        // No rows read yet - start reading at first page. If the stream is unpinned, we can
        // use the reservation saved in PrepareForReadWrite() to pin the first page.
        read_page_ = pages_.begin();
        if (NeedReadReservation(pinned_, num_pages_, true, false) &&
            !NeedReadReservation(pinned_, num_pages_, true, true)) {
            buffer_pool_client_->RestoreReservation(&read_page_reservation_, default_page_len_);
        }
    } else if (delete_on_read_) {
        DCHECK(read_page_ == pages_.begin()) << read_page_->DebugString() << " " << DebugString();
        DCHECK_NE(&*read_page_, write_page_);
        bytes_pinned_ -= pages_.front().len();
        buffer_pool_->DestroyPage(buffer_pool_client_, &pages_.front().handle);
        pages_.pop_front();
        --num_pages_;
        read_page_ = pages_.begin();
    } else {
        // Unpin pages after reading them if needed.
        Page* prev_read_page = &*read_page_;
        ++read_page_;
        UnpinPageIfNeeded(prev_read_page, pinned_);
    }

    if (read_page_ == pages_.end()) {
        CHECK_CONSISTENCY_FULL();
        return Status::OK();
    }

    if (!pinned_ && read_page_->len() > default_page_len_ &&
        buffer_pool_client_->GetUnusedReservation() < read_page_->len()) {
        // If we are iterating over an unpinned stream and encounter a page that is larger
        // than the default page length, then unpinning the previous page may not have
        // freed up enough reservation to pin the next one. The client is responsible for
        // ensuring the reservation is available, so this indicates a bug.
        std::stringstream err_stream;
        err_stream << "Internal error: couldn't pin large page of " << read_page_->len()
                   << " bytes, client only had " << buffer_pool_client_->GetUnusedReservation()
                   << " bytes of unused reservation:" << buffer_pool_client_->DebugString() << "\n";
        return Status::InternalError(err_stream.str());
    }
    // Ensure the next page is pinned for reading. By this point we should have enough
    // reservation to pin the page. If the stream is pinned, the page is already pinned.
    // If the stream is unpinned, we freed up enough memory for a default-sized page by
    // deleting or unpinning the previous page and ensured that, if the page was larger,
    // that the reservation is available with the above check.
    RETURN_IF_ERROR(PinPageIfNeeded(&*read_page_, pinned_));

    // This waits for the pin to complete if the page was unpinned earlier.
    const BufferHandle* read_buffer;
    RETURN_IF_ERROR(read_page_->GetBuffer(&read_buffer));

    read_page_rows_returned_ = 0;
    read_ptr_ = read_buffer->data();
    read_end_ptr_ = read_ptr_ + read_buffer->len();

    // We may need to save reservation for the write page in the case when the write page
    // became a read/write page.
    if (!NeedWriteReservation(pinned_, num_pages_, has_write_iterator(), write_page_ != nullptr,
                              false) &&
        NeedWriteReservation(pinned_, num_pages_, has_write_iterator(), write_page_ != nullptr,
                             has_read_write_page())) {
        buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
    }
    CHECK_CONSISTENCY_FAST();
    return Status::OK();
}

void BufferedTupleStream3::InvalidateReadIterator() {
    if (read_page_ != pages_.end()) {
        // Unpin the write page if we're reading in unpinned mode.
        Page* prev_read_page = &*read_page_;
        read_page_ = pages_.end();
        read_ptr_ = nullptr;
        read_end_ptr_ = nullptr;

        // May need to decrement pin count after destroying read iterator.
        UnpinPageIfNeeded(prev_read_page, pinned_);
    }
    has_read_iterator_ = false;
    if (read_page_reservation_.GetReservation() > 0) {
        buffer_pool_client_->RestoreReservation(&read_page_reservation_, default_page_len_);
    }
    // It is safe to re-read a delete-on-read stream if no rows were read and no pages
    // were therefore deleted.
    if (rows_returned_ == 0) delete_on_read_ = false;
}

Status BufferedTupleStream3::PrepareForRead(bool delete_on_read, bool* got_reservation) {
    CHECK_CONSISTENCY_FULL();
    InvalidateWriteIterator();
    InvalidateReadIterator();
    // If already pinned, no additional pin is needed (see ExpectedPinCount()).
    *got_reservation = pinned_ || pages_.empty() ||
                       buffer_pool_client_->IncreaseReservationToFit(default_page_len_);
    if (!*got_reservation) return Status::OK();
    return PrepareForReadInternal(delete_on_read);
}

Status BufferedTupleStream3::PrepareForReadInternal(bool delete_on_read) {
    DCHECK(!closed_);
    DCHECK(!delete_on_read_);
    DCHECK(!has_read_iterator());

    has_read_iterator_ = true;
    if (pages_.empty()) {
        // No rows to return, or a the first read/write page has not yet been allocated.
        read_page_ = pages_.end();
        read_ptr_ = nullptr;
        read_end_ptr_ = nullptr;
    } else {
        // Eagerly pin the first page in the stream.
        read_page_ = pages_.begin();
        // Check if we need to increment the pin count of the read page.
        RETURN_IF_ERROR(PinPageIfNeeded(&*read_page_, pinned_));
        DCHECK(read_page_->is_pinned());

        // This waits for the pin to complete if the page was unpinned earlier.
        const BufferHandle* read_buffer;
        RETURN_IF_ERROR(read_page_->GetBuffer(&read_buffer));
        read_ptr_ = read_buffer->data();
        read_end_ptr_ = read_ptr_ + read_buffer->len();
    }
    read_page_rows_returned_ = 0;
    rows_returned_ = 0;
    delete_on_read_ = delete_on_read;
    CHECK_CONSISTENCY_FULL();
    return Status::OK();
}

Status BufferedTupleStream3::PinStream(bool* pinned) {
    DCHECK(!closed_);
    CHECK_CONSISTENCY_FULL();
    if (pinned_) {
        *pinned = true;
        return Status::OK();
    }
    *pinned = false;
    // First, make sure we have the reservation to pin all the pages for reading.
    int64_t bytes_to_pin = 0;
    for (Page& page : pages_) {
        bytes_to_pin += (ExpectedPinCount(true, &page) - page.pin_count()) * page.len();
    }

    // Check if we have some reservation to restore.
    bool restore_write_reservation = NeedWriteReservation(false) && !NeedWriteReservation(true);
    bool restore_read_reservation = NeedReadReservation(false) && !NeedReadReservation(true);
    int64_t increase_needed = bytes_to_pin - (restore_write_reservation ? default_page_len_ : 0) -
                              (restore_read_reservation ? default_page_len_ : 0);
    bool reservation_granted = buffer_pool_client_->IncreaseReservationToFit(increase_needed);
    if (!reservation_granted) return Status::OK();

    // If there is no current write page we should have some saved reservation to use.
    // Only continue saving it if the stream is empty and need it to pin the first page.
    if (restore_write_reservation) {
        buffer_pool_client_->RestoreReservation(&write_page_reservation_, default_page_len_);
    }
    if (restore_read_reservation) {
        buffer_pool_client_->RestoreReservation(&read_page_reservation_, default_page_len_);
    }

    // At this point success is guaranteed - go through to pin the pages we need to pin.
    // If the page data was evicted from memory, the read I/O can happen in parallel
    // because we defer calling GetBuffer() until NextReadPage().
    for (Page& page : pages_) RETURN_IF_ERROR(PinPageIfNeeded(&page, true));

    pinned_ = true;
    *pinned = true;
    CHECK_CONSISTENCY_FULL();
    return Status::OK();
}
/*
void BufferedTupleStream3::UnpinStream(UnpinMode mode) {
  CHECK_CONSISTENCY_FULL();
  DCHECK(!closed_);
  if (mode == UNPIN_ALL) {
    // Invalidate the iterators so they don't keep pages pinned.
    InvalidateWriteIterator();
    InvalidateReadIterator();
  }

  if (pinned_) {
    CHECK_CONSISTENCY_FULL();
    // If the stream was pinned, there may be some remaining pinned pages that should
    // be unpinned at this point.
    for (Page& page : pages_) UnpinPageIfNeeded(&page, false);

    // Check to see if we need to save some of the reservation we freed up.
    if (!NeedWriteReservation(true) && NeedWriteReservation(false)) {
      buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
    }
    if (!NeedReadReservation(true) && NeedReadReservation(false)) {
      buffer_pool_client_->SaveReservation(&read_page_reservation_, default_page_len_);
    }
    pinned_ = false;
  }
  CHECK_CONSISTENCY_FULL();
}
*/
Status BufferedTupleStream3::GetRows(std::unique_ptr<RowBatch>* batch, bool* got_rows) {
    if (num_rows() > numeric_limits<int>::max()) {
        // RowBatch::num_rows_ is a 32-bit int, avoid an overflow.
        return Status::InternalError(
                Substitute("Trying to read $0 rows into in-memory batch failed. Limit "
                           "is $1",
                           num_rows(), numeric_limits<int>::max()));
    }
    RETURN_IF_ERROR(PinStream(got_rows));
    if (!*got_rows) return Status::OK();
    bool got_reservation;
    RETURN_IF_ERROR(PrepareForRead(false, &got_reservation));
    DCHECK(got_reservation) << "Stream was pinned";

    // TODO chenhao
    // capacity in RowBatch use int, but _num_rows is int64_t
    // it may be precision loss
    batch->reset(new RowBatch(*desc_, num_rows()));
    bool eos = false;
    // Loop until GetNext fills the entire batch. Each call can stop at page
    // boundaries. We generally want it to stop, so that pages can be freed
    // as we read. It is safe in this case because we pin the entire stream.
    while (!eos) {
        RETURN_IF_ERROR(GetNext(batch->get(), &eos));
    }
    return Status::OK();
}

Status BufferedTupleStream3::GetNext(RowBatch* batch, bool* eos) {
    return GetNextInternal<false>(batch, eos, nullptr);
}

Status BufferedTupleStream3::GetNext(RowBatch* batch, bool* eos, vector<FlatRowPtr>* flat_rows) {
    return GetNextInternal<true>(batch, eos, flat_rows);
}

template <bool FILL_FLAT_ROWS>
Status BufferedTupleStream3::GetNextInternal(RowBatch* batch, bool* eos,
                                             vector<FlatRowPtr>* flat_rows) {
    if (has_nullable_tuple_) {
        return GetNextInternal<FILL_FLAT_ROWS, true>(batch, eos, flat_rows);
    } else {
        return GetNextInternal<FILL_FLAT_ROWS, false>(batch, eos, flat_rows);
    }
}

template <bool FILL_FLAT_ROWS, bool HAS_NULLABLE_TUPLE>
Status BufferedTupleStream3::GetNextInternal(RowBatch* batch, bool* eos,
                                             vector<FlatRowPtr>* flat_rows) {
    DCHECK(!closed_);
    DCHECK(batch->row_desc().equals(*desc_));
    DCHECK(is_pinned() || !FILL_FLAT_ROWS) << "FlatRowPtrs are only valid for pinned streams";
    *eos = (rows_returned_ == num_rows_);
    if (*eos) return Status::OK();

    if (UNLIKELY(read_page_ == pages_.end() || read_page_rows_returned_ == read_page_->num_rows)) {
        // Get the next page in the stream (or the first page if read_page_ was not yet
        // initialized.) We need to do this at the beginning of the GetNext() call to ensure
        // the buffer management semantics. NextReadPage() may unpin or delete the buffer
        // backing the rows returned from the *previous* call to GetNext().
        RETURN_IF_ERROR(NextReadPage());
    }

    DCHECK(has_read_iterator());
    DCHECK(read_page_ != pages_.end());
    DCHECK(read_page_->is_pinned()) << DebugString();
    DCHECK_GE(read_page_rows_returned_, 0);

    int rows_left_in_page = read_page_->num_rows - read_page_rows_returned_;
    int rows_to_fill = std::min(batch->capacity() - batch->num_rows(), rows_left_in_page);
    DCHECK_GE(rows_to_fill, 1);
    uint8_t* tuple_row_mem = reinterpret_cast<uint8_t*>(batch->get_row(batch->num_rows()));

    // Produce tuple rows from the current page and the corresponding position on the
    // null tuple indicator.
    if (FILL_FLAT_ROWS) {
        DCHECK(flat_rows != nullptr);
        DCHECK(!delete_on_read_);
        DCHECK_EQ(batch->num_rows(), 0);
        flat_rows->clear();
        flat_rows->reserve(rows_to_fill);
    }

    const uint64_t tuples_per_row = desc_->tuple_descriptors().size();
    // Start reading from the current position in 'read_page_'.
    for (int i = 0; i < rows_to_fill; ++i) {
        if (FILL_FLAT_ROWS) {
            flat_rows->push_back(read_ptr_);
            DCHECK_EQ(flat_rows->size(), i + 1);
        }
        // Copy the row into the output batch.
        TupleRow* output_row = reinterpret_cast<TupleRow*>(tuple_row_mem);
        tuple_row_mem += sizeof(Tuple*) * tuples_per_row;
        UnflattenTupleRow<HAS_NULLABLE_TUPLE>(&read_ptr_, output_row);

        // Update string slot ptrs, skipping external strings.
        for (int j = 0; j < inlined_string_slots_.size(); ++j) {
            Tuple* tuple = output_row->get_tuple(inlined_string_slots_[j].first);
            if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
            FixUpStringsForRead(inlined_string_slots_[j].second, tuple);
        }
        /*
    // Update collection slot ptrs, skipping external collections. We traverse the
    // collection structure in the same order as it was written to the stream, allowing
    // us to infer the data layout based on the length of collections and strings.
    for (int j = 0; j < inlined_coll_slots_.size(); ++j) {
      Tuple* tuple = output_row->get_tuple(inlined_coll_slots_[j].first);
      if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
      FixUpCollectionsForRead(inlined_coll_slots_[j].second, tuple);
    }
*/
    }

    batch->commit_rows(rows_to_fill);
    rows_returned_ += rows_to_fill;
    read_page_rows_returned_ += rows_to_fill;
    *eos = (rows_returned_ == num_rows_);
    if (read_page_rows_returned_ == read_page_->num_rows && (!pinned_ || delete_on_read_)) {
        // No more data in this page. The batch must be immediately returned up the operator
        // tree and deep copied so that NextReadPage() can reuse the read page's buffer.
        // TODO: IMPALA-4179 - instead attach the buffer and flush the resources.
        batch->mark_needs_deep_copy();
    }
    if (FILL_FLAT_ROWS) DCHECK_EQ(flat_rows->size(), rows_to_fill);
    DCHECK_LE(read_ptr_, read_end_ptr_);
    return Status::OK();
}

void BufferedTupleStream3::FixUpStringsForRead(const vector<SlotDescriptor*>& string_slots,
                                               Tuple* tuple) {
    DCHECK(tuple != nullptr);
    for (const SlotDescriptor* slot_desc : string_slots) {
        if (tuple->is_null(slot_desc->null_indicator_offset())) continue;

        StringValue* sv = tuple->get_string_slot(slot_desc->tuple_offset());
        DCHECK_LE(read_ptr_ + sv->len, read_end_ptr_);
        sv->ptr = reinterpret_cast<char*>(read_ptr_);
        read_ptr_ += sv->len;
    }
}
/*
void BufferedTupleStream3::FixUpCollectionsForRead(
    const vector<SlotDescriptor*>& collection_slots, Tuple* tuple) {
  DCHECK(tuple != nullptr);
  for (const SlotDescriptor* slot_desc : collection_slots) {
    if (tuple->is_null(slot_desc->null_indicator_offset())) continue;

    CollectionValue* cv = tuple->get_collection_slot(slot_desc->tuple_offset());
    const TupleDescriptor& item_desc = *slot_desc->collection_item_descriptor();
    int coll_byte_size = cv->num_tuples * item_desc.byte_size();
    DCHECK_LE(read_ptr_ + coll_byte_size, read_end_ptr_);
    cv->ptr = reinterpret_cast<uint8_t*>(read_ptr_);
    read_ptr_ += coll_byte_size;

    if (!item_desc.has_varlen_slots()) continue;
    uint8_t* coll_data = cv->ptr;
    for (int i = 0; i < cv->num_tuples; ++i) {
      Tuple* item = reinterpret_cast<Tuple*>(coll_data);
      FixUpStringsForRead(item_desc.string_slots(), item);
      FixUpCollectionsForRead(item_desc.collection_slots(), item);
      coll_data += item_desc.byte_size();
    }
  }
}
*/
int64_t BufferedTupleStream3::ComputeRowSize(TupleRow* row) const noexcept {
    int64_t size = 0;
    if (has_nullable_tuple_) {
        size += NullIndicatorBytesPerRow();
        for (int i = 0; i < fixed_tuple_sizes_.size(); ++i) {
            if (row->get_tuple(i) != nullptr) size += fixed_tuple_sizes_[i];
        }
    } else {
        for (int i = 0; i < fixed_tuple_sizes_.size(); ++i) {
            size += fixed_tuple_sizes_[i];
        }
    }
    for (int i = 0; i < inlined_string_slots_.size(); ++i) {
        Tuple* tuple = row->get_tuple(inlined_string_slots_[i].first);
        if (tuple == nullptr) continue;
        const vector<SlotDescriptor*>& slots = inlined_string_slots_[i].second;
        for (auto it = slots.begin(); it != slots.end(); ++it) {
            if (tuple->is_null((*it)->null_indicator_offset())) continue;
            size += tuple->get_string_slot((*it)->tuple_offset())->len;
        }
    }

    /*
  for (int i = 0; i < inlined_coll_slots_.size(); ++i) {
    Tuple* tuple = row->get_tuple(inlined_coll_slots_[i].first);
    if (tuple == nullptr) continue;
    const vector<SlotDescriptor*>& slots = inlined_coll_slots_[i].second;
    for (auto it = slots.begin(); it != slots.end(); ++it) {
      if (tuple->is_null((*it)->null_indicator_offset())) continue;
      CollectionValue* cv = tuple->get_collection_slot((*it)->tuple_offset());
      const TupleDescriptor& item_desc = *(*it)->collection_item_descriptor();
      size += cv->num_tuples * item_desc.byte_size();

      if (!item_desc.has_varlen_slots()) continue;
      for (int j = 0; j < cv->num_tuples; ++j) {
        Tuple* item = reinterpret_cast<Tuple*>(&cv->ptr[j * item_desc.byte_size()]);
        size += item->varlen_byte_size(item_desc);
      }
    }
  }
*/
    return size;
}

bool BufferedTupleStream3::AddRowSlow(TupleRow* row, Status* status) noexcept {
    // Use AddRowCustom*() to do the work of advancing the page.
    int64_t row_size = ComputeRowSize(row);
    uint8_t* data = AddRowCustomBeginSlow(row_size, status);
    if (data == nullptr) return false;
    bool success = DeepCopy(row, &data, data + row_size);
    DCHECK(success);
    DCHECK_EQ(data, write_ptr_);
    AddRowCustomEnd(row_size);
    return true;
}

uint8_t* BufferedTupleStream3::AddRowCustomBeginSlow(int64_t size, Status* status) noexcept {
    bool got_reservation = false;
    *status = AdvanceWritePage(size, &got_reservation);
    if (!status->ok() || !got_reservation) {
        return nullptr;
    }
    // We have a large-enough page so now success is guaranteed.
    uint8_t* result = AddRowCustomBegin(size, status);
    DCHECK(result != nullptr);
    return result;
}

void BufferedTupleStream3::AddLargeRowCustomEnd(int64_t size) noexcept {
    DCHECK_GT(size, default_page_len_);
    // Immediately unpin the large write page so that we're not using up extra reservation
    // and so we don't append another row to the page.
    ResetWritePage();
    // Save some of the reservation we freed up so we can create the next write page when
    // needed.
    if (NeedWriteReservation()) {
        buffer_pool_client_->SaveReservation(&write_page_reservation_, default_page_len_);
    }
    // The stream should be in a consistent state once the row is added.
    CHECK_CONSISTENCY_FAST();
}

bool BufferedTupleStream3::AddRow(TupleRow* row, Status* status) noexcept {
    DCHECK(!closed_);
    DCHECK(has_write_iterator());
    if (UNLIKELY(write_page_ == nullptr || !DeepCopy(row, &write_ptr_, write_end_ptr_))) {
        return AddRowSlow(row, status);
    }
    ++num_rows_;
    ++write_page_->num_rows;
    return true;
}

bool BufferedTupleStream3::DeepCopy(TupleRow* row, uint8_t** data,
                                    const uint8_t* data_end) noexcept {
    return has_nullable_tuple_ ? DeepCopyInternal<true>(row, data, data_end)
                               : DeepCopyInternal<false>(row, data, data_end);
}

// TODO: consider codegening this.
// TODO: in case of duplicate tuples, this can redundantly serialize data.
template <bool HAS_NULLABLE_TUPLE>
bool BufferedTupleStream3::DeepCopyInternal(TupleRow* row, uint8_t** data,
                                            const uint8_t* data_end) noexcept {
    uint8_t* pos = *data;
    const uint64_t tuples_per_row = desc_->tuple_descriptors().size();
    // Copy the not nullptr fixed len tuples. For the nullptr tuples just update the nullptr tuple
    // indicator.
    if (HAS_NULLABLE_TUPLE) {
        int null_indicator_bytes = NullIndicatorBytesPerRow();
        if (UNLIKELY(pos + null_indicator_bytes > data_end)) return false;
        uint8_t* null_indicators = pos;
        pos += NullIndicatorBytesPerRow();
        memset(null_indicators, 0, null_indicator_bytes);
        for (int i = 0; i < tuples_per_row; ++i) {
            uint8_t* null_word = null_indicators + (i >> 3);
            const uint32_t null_pos = i & 7;
            const int tuple_size = fixed_tuple_sizes_[i];
            Tuple* t = row->get_tuple(i);
            const uint8_t mask = 1 << (7 - null_pos);
            if (t != nullptr) {
                if (UNLIKELY(pos + tuple_size > data_end)) return false;
                memcpy(pos, t, tuple_size);
                pos += tuple_size;
            } else {
                *null_word |= mask;
            }
        }
    } else {
        // If we know that there are no nullable tuples no need to set the nullability flags.
        for (int i = 0; i < tuples_per_row; ++i) {
            const int tuple_size = fixed_tuple_sizes_[i];
            if (UNLIKELY(pos + tuple_size > data_end)) return false;
            Tuple* t = row->get_tuple(i);
            // TODO: Once IMPALA-1306 (Avoid passing empty tuples of non-materialized slots)
            // is delivered, the check below should become DCHECK(t != nullptr).
            DCHECK(t != nullptr || tuple_size == 0);
            memcpy(pos, t, tuple_size);
            pos += tuple_size;
        }
    }

    // Copy inlined string slots. Note: we do not need to convert the string ptrs to offsets
    // on the write path, only on the read. The tuple data is immediately followed
    // by the string data so only the len information is necessary.
    for (int i = 0; i < inlined_string_slots_.size(); ++i) {
        const Tuple* tuple = row->get_tuple(inlined_string_slots_[i].first);
        if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
        if (UNLIKELY(!CopyStrings(tuple, inlined_string_slots_[i].second, &pos, data_end)))
            return false;
    }
    /*
  // Copy inlined collection slots. We copy collection data in a well-defined order so
  // we do not need to convert pointers to offsets on the write path.
  for (int i = 0; i < inlined_coll_slots_.size(); ++i) {
    const Tuple* tuple = row->get_tuple(inlined_coll_slots_[i].first);
    if (HAS_NULLABLE_TUPLE && tuple == nullptr) continue;
    if (UNLIKELY(!CopyCollections(tuple, inlined_coll_slots_[i].second, &pos, data_end)))
      return false;
  }
*/
    *data = pos;
    return true;
}

bool BufferedTupleStream3::CopyStrings(const Tuple* tuple,
                                       const vector<SlotDescriptor*>& string_slots, uint8_t** data,
                                       const uint8_t* data_end) {
    for (const SlotDescriptor* slot_desc : string_slots) {
        if (tuple->is_null(slot_desc->null_indicator_offset())) continue;
        const StringValue* sv = tuple->get_string_slot(slot_desc->tuple_offset());
        if (LIKELY(sv->len > 0)) {
            if (UNLIKELY(*data + sv->len > data_end)) return false;

            memcpy(*data, sv->ptr, sv->len);
            *data += sv->len;
        }
    }
    return true;
}
/*
bool BufferedTupleStream3::CopyCollections(const Tuple* tuple,
    const vector<SlotDescriptor*>& collection_slots, uint8_t** data, const uint8_t* data_end) {
  for (const SlotDescriptor* slot_desc : collection_slots) {
    if (tuple->is_null(slot_desc->null_indicator_offset())) continue;
    const CollectionValue* cv = tuple->get_collection_slot(slot_desc->tuple_offset());
    const TupleDescriptor& item_desc = *slot_desc->collection_item_descriptor();
    if (LIKELY(cv->num_tuples > 0)) {
      int coll_byte_size = cv->num_tuples * item_desc.byte_size();
      if (UNLIKELY(*data + coll_byte_size > data_end)) return false;
      uint8_t* coll_data = *data;
      memcpy(coll_data, cv->ptr, coll_byte_size);
      *data += coll_byte_size;

      if (!item_desc.has_varlen_slots()) continue;
      // Copy variable length data when present in collection items.
      for (int i = 0; i < cv->num_tuples; ++i) {
        const Tuple* item = reinterpret_cast<Tuple*>(coll_data);
        if (UNLIKELY(!CopyStrings(item, item_desc.string_slots(), data, data_end))) {
          return false;
        }
        if (UNLIKELY(
                !CopyCollections(item, item_desc.collection_slots(), data, data_end))) {
          return false;
        }
        coll_data += item_desc.byte_size();
      }
    }
  }
  return true;
}
*/
void BufferedTupleStream3::GetTupleRow(FlatRowPtr flat_row, TupleRow* row) const {
    DCHECK(row != nullptr);
    DCHECK(!closed_);
    DCHECK(is_pinned());
    DCHECK(!delete_on_read_);
    uint8_t* data = flat_row;
    return has_nullable_tuple_ ? UnflattenTupleRow<true>(&data, row)
                               : UnflattenTupleRow<false>(&data, row);
}

template <bool HAS_NULLABLE_TUPLE>
void BufferedTupleStream3::UnflattenTupleRow(uint8_t** data, TupleRow* row) const {
    const int tuples_per_row = desc_->tuple_descriptors().size();
    uint8_t* ptr = *data;
    if (has_nullable_tuple_) {
        // Stitch together the tuples from the page and the nullptr ones.
        const uint8_t* null_indicators = ptr;
        ptr += NullIndicatorBytesPerRow();
        for (int i = 0; i < tuples_per_row; ++i) {
            const uint8_t* null_word = null_indicators + (i >> 3);
            const uint32_t null_pos = i & 7;
            const bool is_not_null = ((*null_word & (1 << (7 - null_pos))) == 0);
            row->set_tuple(i,
                           reinterpret_cast<Tuple*>(reinterpret_cast<uint64_t>(ptr) * is_not_null));
            ptr += fixed_tuple_sizes_[i] * is_not_null;
        }
    } else {
        for (int i = 0; i < tuples_per_row; ++i) {
            row->set_tuple(i, reinterpret_cast<Tuple*>(ptr));
            ptr += fixed_tuple_sizes_[i];
        }
    }
    *data = ptr;
}
