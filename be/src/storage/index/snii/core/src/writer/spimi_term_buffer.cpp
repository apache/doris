#include "snii/writer/spimi_term_buffer.h"

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cstdio>
#include <cstdlib>
#include <memory>
#include <numeric>
#include <string>
#include <utility>

#include "snii/encoding/varint.h"
#include "snii/format/format_constants.h"
#include "snii/writer/spill_run_codec.h"
#include "snii/writer/temp_dir.h"

#if defined(__GLIBC__)
#include <malloc.h>
#endif

namespace snii::writer {

namespace {

// Returns freed heap arenas to the OS (glibc only). The spill encode churns many
// small allocations whose freed chunks glibc retains in its arenas; trimming
// before the peak-RSS-defining merge phase recovers that retention. No-op (and
// harmless) on non-glibc libcs.
void TrimMalloc() {
#if defined(__GLIBC__)
    ::malloc_trim(0);
#endif
}

// Process-unique temp path for a spill run under `dir` (pid + monotonic counter so
// parallel builds / multiple buffers never collide).
std::string MakeRunPath(const std::string& dir) {
    static std::atomic<uint64_t> counter {0};
    const uint64_t n = counter.fetch_add(1);
    return dir + "/snii_spill_" + std::to_string(::getpid()) + "_" + std::to_string(n) + ".run";
}

} // namespace

SpimiTermBuffer::SpimiTermBuffer(const std::vector<std::string>* vocab, bool has_positions,
                                 size_t spill_threshold_bytes, MemoryReporter* reporter)
        : vocab_(vocab),
          has_positions_(has_positions),
          spill_threshold_bytes_(spill_threshold_bytes),
          mem_reporter_(reporter) {
    // Borrowed-vocab mode: only the 4 B/id slot-index array is sized to the
    // vocabulary; the Term pool (slots_) grows with the LIVE touched count, so an
    // all-but-empty vocabulary costs ~4 B/id instead of ~80 B/id.
    slot_of_.assign(vocab_->size(), 0);
    // The vocab-sized slot index is resident immediately and survives spills; report
    // its initial positive delta now.
    report_arena_delta();
}

SpimiTermBuffer::SpimiTermBuffer(bool has_positions, size_t spill_threshold_bytes,
                                 MemoryReporter* reporter)
        : vocab_(&owned_vocab_),
          has_positions_(has_positions),
          spill_threshold_bytes_(spill_threshold_bytes),
          mem_reporter_(reporter) {
    // Owned-vocab mode: the vocabulary grows as strings are interned; terms_ /
    // present_ grow alongside it in add_token(string_view, ...).
}

SpimiTermBuffer::~SpimiTermBuffer() {
    // Balance the writer-level / Doris tracker on the error path: if the buffer is
    // destroyed while resident bytes were reported but not yet freed-and-reported
    // (e.g. a build aborts before draining), return them here so nothing leaks.
    if (mem_reporter_ != nullptr && reported_resident_ != 0) {
        mem_reporter_->report(-reported_resident_);
        reported_resident_ = 0;
    }
    cleanup_runs();
}

void SpimiTermBuffer::report_arena_delta() {
    if (mem_reporter_ == nullptr) return;
    // Diff the REAL resident bytes (arena + slot index) against the last reported
    // total; emit the signed delta exactly once.
    const int64_t now = static_cast<int64_t>(resident_bytes());
    mem_reporter_->report(now - reported_resident_);
    reported_resident_ = now;
}

size_t SpimiTermBuffer::unique_terms() const {
    return live_term_count_;
}

uint64_t SpimiTermBuffer::resident_bytes() const {
    // REAL resident accumulator bytes: the posting arena plus the vocab-sized slot
    // index (capacity, since the reserved-but-unused tail is still resident RSS and
    // survives spills -- spill_to_run does NOT free slot_of_). This is the gate-2
    // spill trigger metric and the spill space-precheck figure -- NOT the old gated
    // live_bytes_ estimate.
    return pool_.arena_bytes() + static_cast<uint64_t>(slot_of_.capacity()) * sizeof(uint32_t);
}

// Returns the live Term for `term_id`, claiming a pool slot on first touch (1 ==
// new). Reuses a freed slot from free_slots_ when available; otherwise appends a
// fresh Term to slots_. slot_of_[term_id] holds (slot index + 1); 0 means empty.
SpimiTermBuffer::Term& SpimiTermBuffer::term_slot(uint32_t term_id, bool* new_term) {
    uint32_t enc = slot_of_[term_id];
    if (enc != 0) {
        *new_term = false;
        return slots_[enc - 1];
    }
    *new_term = true;
    uint32_t slot;
    if (!free_slots_.empty()) {
        slot = free_slots_.back();
        free_slots_.pop_back();
    } else {
        slot = static_cast<uint32_t>(slots_.size());
        slots_.emplace_back();
    }
    slot_of_[term_id] = slot + 1;
    return slots_[slot];
}

// Appends one byte to a term's chain, starting the chain lazily on first use.
void SpimiTermBuffer::put_byte(Term* t, uint8_t b) {
    if (t->head == kNoChain) t->head = pool_.start_chain(&t->w, &t->level);
    pool_.append_byte(&t->w, &t->level, b);
}

void SpimiTermBuffer::put_varint(Term* t, uint64_t v) {
    uint8_t tmp[10];
    const size_t n = encode_varint64(v, tmp);
    for (size_t i = 0; i < n; ++i) put_byte(t, tmp[i]);
}

void SpimiTermBuffer::accumulate(uint32_t term_id, uint32_t docid, uint32_t pos) {
    bool new_term = false;
    Term& t = term_slot(term_id, &new_term);
    if (new_term) {
        touched_ids_.push_back(term_id);
        ++live_term_count_;
    }
    // A token starts a new doc unless it continues the most-recent doc for this term.
    const bool new_doc = !t.started || t.cur_docid != docid;
    // Tagged entry: varint((pos << 1) | new_doc). Positions are tagged 0 when
    // disabled. The new_doc bit lets the decoder recover per-doc freqs by counting.
    // Widen to 64-bit so a full 32-bit position survives the << 1 without truncation.
    const uint64_t tagged = has_positions_
                                    ? ((static_cast<uint64_t>(pos) << 1) | (new_doc ? 1u : 0u))
                                    : (new_doc ? 1u : 0u);
    put_varint(&t, tagged);
    if (new_doc) {
        // Out-of-order docids are tolerated (zigzag delta is signed) and reordered at
        // finalize; flag them so to_postings sorts. The delta base is the previous
        // distinct doc (cur_docid), which is 0 for the very first doc (started==false).
        const int64_t base = t.started ? static_cast<int64_t>(t.cur_docid) : 0;
        if (t.started && docid < t.cur_docid) t.sorted = false;
        const int64_t delta = static_cast<int64_t>(docid) - base;
        put_varint(&t, zigzag_encode(delta));
        t.cur_docid = docid;
        t.started = true;
    }
    ++t.ntok;
    ++total_tokens_;

    // Gate-2 spill: trigger on REAL resident bytes (arena + slot index), NOT the old
    // gated live_bytes_ estimate. arena_bytes() is monotonic per fill and reset to 0
    // by spill_to_run()'s pool_.reset(), so the trigger self-rearms after each spill.
    // The OTHER trigger is the hard arena safety stop (active even in unlimited mode):
    // when the arena nears the 4 GiB uint32-offset limit -- without it, a single
    // >4 GiB in-memory segment wraps alloc_run and silently corrupts data. A forced
    // spill + final k-way merge stays byte-identical regardless of when it fires.
    constexpr uint64_t kArenaSpillCap = 0xE0000000ull; // 3.5 GiB, < UINT32_MAX margin
    // Report this token's REAL resident growth FIRST so the writer's unified total
    // (reporter_->current_bytes()) reflects it before the gate-2 check. Single-source
    // diff: cheap (subtraction + relaxed atomic add; arena_bytes() is two field reads).
    report_arena_delta();
    // Gate-2 spill (UNIFIED): when a reporter is attached, trigger on the writer's TOTAL
    // build RAM (arena + slot index + dict) crossing the one configured cap -- the same
    // total and cap every buffer of this writer shares, not a per-buffer threshold. Off
    // Doris (no reporter) fall back to the local spill_threshold_bytes_. The hard arena
    // safety stop (4 GiB uint32-offset limit) is always active. spill_to_run() resets the
    // arena and reports its negative internally, so the unified total drops after a spill.
    const bool over_cap = mem_reporter_ != nullptr ? mem_reporter_->over_cap()
                                                   : (spill_threshold_bytes_ != 0 &&
                                                      resident_bytes() >= spill_threshold_bytes_);
    const bool arena_near_limit = pool_.arena_bytes() >= kArenaSpillCap;
    if ((over_cap || arena_near_limit) && spill_status_.ok()) {
        spill_status_ = spill_to_run();
    }
}

void SpimiTermBuffer::add_token(uint32_t term_id, uint32_t docid, uint32_t pos) {
    // Hot path: a pooled slot lookup + a couple of pushes. No hashing, no string
    // construction per token. Reject (and latch) an out-of-range id.
    if (term_id >= slot_of_.size()) {
        if (spill_status_.ok()) {
            spill_status_ = Status::InvalidArgument("spimi: term_id out of vocab range");
        }
        return;
    }
    accumulate(term_id, docid, pos);
}

void SpimiTermBuffer::add_token(std::string_view term, uint32_t docid, uint32_t pos) {
    // Compatibility path: intern the term into the owned vocabulary on first
    // occurrence, then accumulate by its id. ONLY valid in OWNED-vocab mode. In
    // BORROWED-vocab mode vocab_ points at the caller's vector, NOT &owned_vocab_:
    // interning here would grow owned_vocab_ / intern_ / slot_of_ out of step with
    // the active (borrowed) vocab, so the new id indexes the WRONG string and writes
    // a slot_of_ entry the borrowed-vocab build never reconciles -- silent
    // corruption. Reject (and latch) instead of forwarding by a bogus id.
    if (vocab_ != &owned_vocab_) {
        if (spill_status_.ok()) {
            spill_status_ = Status::InvalidArgument(
                    "spimi: add_token(string_view) requires owned-vocab mode");
        }
        return;
    }
    auto it = intern_.find(std::string(term));
    uint32_t term_id;
    if (it == intern_.end()) {
        term_id = static_cast<uint32_t>(owned_vocab_.size());
        owned_vocab_.emplace_back(term);
        intern_.emplace(owned_vocab_.back(), term_id);
        slot_of_.push_back(0); // vocab grows: new id starts with no live slot
    } else {
        term_id = it->second;
    }
    accumulate(term_id, docid, pos);
}

namespace {

// Reorders a term's flat arrays into ascending-docid order, COALESCING any
// same-docid groups so the result has exactly one entry per docid -- matching the
// k-way-merge path's boundary-doc coalescing and the writer's strictly-ascending
// precondition. Only invoked for the rare term that received out-of-order docids
// (the common ascending path leaves t.sorted true and skips it).
//
// A docid may REVISIT (e.g. feed 5,1,5): the chain holds two separate doc-groups
// for doc 5. A STABLE sort keeps equal-docid groups in arrival order, then the
// coalesce pass sums their freqs and concatenates their positions in that same
// (document/arrival) order -- so the merged positions stay consistent with the
// merged freqs, exactly as the run-order merge would have produced.
void SortByDocid(std::vector<uint32_t>* docids, std::vector<uint32_t>* freqs,
                 std::vector<uint32_t>* positions_flat, bool has_positions) {
    const size_t n = docids->size();
    std::vector<size_t> order(n);
    std::iota(order.begin(), order.end(), 0);
    // STABLE so equal docids keep arrival order: their positions then concatenate in
    // document order, the same order the merge path's run concatenation yields.
    std::stable_sort(order.begin(), order.end(),
                     [&](size_t a, size_t b) { return (*docids)[a] < (*docids)[b]; });

    std::vector<uint32_t> pos_off;
    if (has_positions) {
        pos_off.resize(n);
        uint32_t running = 0;
        for (size_t i = 0; i < n; ++i) {
            pos_off[i] = running;
            running += (*freqs)[i];
        }
    }
    std::vector<uint32_t> nd, nf, np;
    nd.reserve(n);
    nf.reserve(n);
    if (has_positions) np.reserve(positions_flat->size());
    for (size_t k : order) {
        // Coalesce a revisited docid into the previous entry (it sorts adjacent now):
        // sum freqs and append this group's positions right after the prior group's,
        // so flat doc order stays partitioned by the merged freqs.
        if (!nd.empty() && nd.back() == (*docids)[k]) {
            nf.back() += (*freqs)[k];
        } else {
            nd.push_back((*docids)[k]);
            nf.push_back((*freqs)[k]);
        }
        if (has_positions) {
            np.insert(np.end(), positions_flat->begin() + pos_off[k],
                      positions_flat->begin() + pos_off[k] + (*freqs)[k]);
        }
    }
    *docids = std::move(nd);
    *freqs = std::move(nf);
    if (has_positions) *positions_flat = std::move(np);
}

} // namespace

namespace {

// Decodes one varint from a pool chain cursor. The chain was written by
// encode_varint*, so the same LEB128 continuation-bit loop reconstructs it.
uint64_t DecodeChainVarint(CompactPostingPool::Cursor* c) {
    uint64_t result = 0;
    int shift = 0;
    for (;;) {
        const uint8_t b = c->next();
        result |= static_cast<uint64_t>(b & 0x7F) << shift;
        if ((b & 0x80) == 0) break;
        shift += 7;
    }
    return result;
}

void SkipChainVarint(CompactPostingPool::Cursor* c) {
    DecodeChainVarint(c);
}

} // namespace

// Decodes a term's compact tagged chain back into a flat TermPostings (the exact
// docids/freqs/positions_flat the writer consumes), so the produced index is
// byte-identical to the legacy raw-uint32 accumulator. The chain holds one entry
// per token: varint((pos << 1) | new_doc); each new_doc entry is followed by a
// zigzag(docid-delta). A doc's freq is the run length of consecutive same-doc
// tokens; positions stream out in document order (empty when positions disabled).
// Stream positions for a sorted term whose token count exceeds this: such a term's
// flat positions buffer (uint32 per token) would be the peak-RSS transient (tens of
// MiB for the widest term). Below it, the flat buffer is cheap and simpler.
static constexpr uint32_t kStreamPositionsTokenThreshold = 1u << 16; // 65536

TermPostings SpimiTermBuffer::to_postings(std::string term, Term&& t,
                                          bool allow_stream_positions) const {
    TermPostings tp;
    tp.term = std::move(term);
    if (t.ntok == 0 || t.head == kNoChain) return tp;

    // Reserve docids/freqs by ntok (an upper bound on the doc count: ntok >= ndocs).
    // The doc count is not stored separately to keep Term compact; since the corpus
    // is freq~1 per (term, doc), ntok ~= ndocs so the over-reserve is negligible.
    tp.docids.reserve(t.ntok);
    tp.freqs.reserve(t.ntok);

    // For a large SORTED term, stream positions on demand instead of materializing a
    // multi-MiB flat buffer: the writer (prx builder) pulls them window by window via
    // pos_pump, decoding straight from the still-resident arena chain. Out-of-order
    // terms (rare, defensive) need a full sort, so they always use the flat path.
    const bool stream_pos = allow_stream_positions && has_positions_ && t.sorted &&
                            t.ntok >= kStreamPositionsTokenThreshold;
    if (has_positions_ && !stream_pos) tp.positions_flat.reserve(t.ntok);

    CompactPostingPool::Cursor c = pool_.cursor(t.head, t.w.cur);
    int64_t prev = 0;
    for (uint32_t i = 0; i < t.ntok; ++i) {
        const uint64_t tagged = DecodeChainVarint(&c);
        const bool new_doc = (tagged & 1u) != 0;
        if (new_doc) {
            prev += zigzag_decode(DecodeChainVarint(&c));
            tp.docids.push_back(static_cast<uint32_t>(prev));
            tp.freqs.push_back(0);
        }
        ++tp.freqs.back(); // count this token toward the current doc's freq
        if (has_positions_ && !stream_pos) {
            tp.positions_flat.push_back(static_cast<uint32_t>(tagged >> 1));
        }
    }

    // Decide the FINAL position handling now that df (= docids.size()) is known.
    // pos_pump is honored ONLY by the windowed writer path (build_windowed_entry),
    // taken when df >= kSlimDfThreshold. A SLIM term (df below it) goes through
    // build_slim_entry, which reads positions_flat directly -- so streaming would
    // leave it empty and crash. A high-ntok but low-df term (many repeats in few
    // docs) therefore falls back to materializing its df-bounded positions here.
    const bool windowed_path = tp.docids.size() >= snii::format::kSlimDfThreshold;
    if (stream_pos && windowed_path) {
        // Hand the writer a sequential position source backed by a SECOND pass over the
        // same chain (the chain stays resident in pool_ for the whole drain). The pump
        // yields positions in document order -- identical to positions_flat -- so the
        // produced .prx is byte-for-byte the same. The cursor is shared/advanced across
        // calls (the writer pulls in order, exactly pos_total positions total).
        tp.pos_total = t.ntok;
        auto cur = std::make_shared<CompactPostingPool::Cursor>(pool_.cursor(t.head, t.w.cur));
        tp.pos_pump = [cur](uint32_t* dst, size_t count) {
            // Re-walk the tagged token stream, yielding one position per token. A new-doc
            // token is followed by a zigzag docid-delta varint that must be consumed and
            // discarded so the cursor stays aligned with the encoding.
            for (size_t k = 0; k < count; ++k) {
                const uint64_t tagged = DecodeChainVarint(cur.get());
                if ((tagged & 1u) != 0) SkipChainVarint(cur.get());
                dst[k] = static_cast<uint32_t>(tagged >> 1);
            }
        };
    } else if (stream_pos && has_positions_) {
        // Slim fallback: the decode loop skipped positions (stream candidate) but the
        // term is slim, so materialize positions_flat in a second pass for build_slim.
        tp.positions_flat.reserve(t.ntok);
        CompactPostingPool::Cursor pc = pool_.cursor(t.head, t.w.cur);
        for (uint32_t i = 0; i < t.ntok; ++i) {
            const uint64_t tagged = DecodeChainVarint(&pc);
            if ((tagged & 1u) != 0) SkipChainVarint(&pc);
            tp.positions_flat.push_back(static_cast<uint32_t>(tagged >> 1));
        }
    } else if (!t.sorted) {
        // Defensive reorder for the rare out-of-order-docid feed (merge of pre-sorted
        // runs). The common ascending path leaves t.sorted true and skips it.
        SortByDocid(&tp.docids, &tp.freqs, &tp.positions_flat, has_positions_);
    }
    return tp;
}

void SpimiTermBuffer::ensure_string_rank() const {
    const std::vector<std::string>& v = vocab();
    if (string_rank_.size() == v.size()) return; // already built (or empty vocab)
    // One full lexicographic sort of the vocabulary, amortized over every spill.
    std::vector<uint32_t> order(v.size());
    std::iota(order.begin(), order.end(), 0u);
    std::sort(order.begin(), order.end(), [&](uint32_t a, uint32_t b) { return v[a] < v[b]; });
    string_rank_.assign(v.size(), 0u);
    for (uint32_t rank = 0; rank < order.size(); ++rank) {
        string_rank_[order[rank]] = rank;
    }
}

std::vector<uint32_t> SpimiTermBuffer::sorted_ids() const {
    ensure_string_rank();
    std::vector<uint32_t> ids = touched_ids_;
    const std::vector<uint32_t>& rank = string_rank_;
    // Integer rank compare instead of full std::string compare: equal-string ids
    // cannot occur for a dense vocab, so a strict rank order matches the original
    // lexicographic order exactly.
    std::sort(ids.begin(), ids.end(), [&](uint32_t a, uint32_t b) { return rank[a] < rank[b]; });
    return ids;
}

void SpimiTermBuffer::release_term(uint32_t term_id) {
    const uint32_t enc = slot_of_[term_id];
    if (enc == 0) return; // not live (defensive)
    const uint32_t slot = enc - 1;
    slots_[slot] = Term(); // free this term's arrays; the empty Term slot is reusable
    free_slots_.push_back(slot);
    slot_of_[term_id] = 0;
    --live_term_count_;
}

Status SpimiTermBuffer::drain_sorted(const std::function<void(TermPostings&&)>& fn,
                                     bool allow_stream_positions) {
    const std::vector<std::string>& v = vocab();
    for (uint32_t id : sorted_ids()) {
        Term term = std::move(slots_[slot_of_[id] - 1]);
        release_term(id); // release this term's slot before building the next
        // Allow streaming positions only when the caller consumes synchronously (the
        // arena chain stays resident for the whole drain, so the pump can read from it).
        TermPostings tp = to_postings(v[id], std::move(term), allow_stream_positions);
        fn(std::move(tp));
    }
    touched_ids_.clear();
    // Drop the arena + the slot pool (their bytes are fully decoded) and return the
    // freed chunks to the OS so the process peak reflects only what survives the
    // drain, not retained input-phase arena memory.
    pool_.reset();
    std::vector<Term>().swap(slots_);
    std::vector<uint32_t>().swap(free_slots_);
    std::vector<uint32_t>().swap(slot_of_);
    TrimMalloc();
    // Arena reset + slot_of_ freed: now real resident ~0, so this emits the final
    // negative that returns every reported byte (no leak after the in-memory drain).
    report_arena_delta();
    return Status::OK();
}

Status SpimiTermBuffer::drain_to_writer(RunWriter* w) {
    Status st = Status::OK();
    const std::vector<std::string>& v = vocab();
    // Spill writes by term-id (no string IO). Iterate touched ids in vocab-string
    // order so each run is sorted; the k-way merge re-orders runs by the same key.
    for (uint32_t id : sorted_ids()) {
        Term term = std::move(slots_[slot_of_[id] - 1]);
        release_term(id);
        // Spill path: the run codec serializes positions_flat directly, so positions
        // must be materialized (no streaming pump).
        TermPostings tp = to_postings(v[id], std::move(term), /*allow_stream=*/false);
        if (st.ok()) st = w->write_term(id, tp);
    }
    touched_ids_.clear();
    pool_.reset(); // all chains decoded into the run; free the arena for the refill
    // The spill returns the arena to 0; slot_of_ keeps its capacity (survives
    // the spill). Report the arena-drop negative now so the gate-2 spill is balanced
    // immediately, not deferred to the next token.
    report_arena_delta();
    return st;
}

Status SpimiTermBuffer::spill_to_run() {
    const std::string dir = resolve_temp_dir();
    // Best-effort space pre-check: fail with a clear, early error rather than a
    // mid-write IoError that leaves a half-written run. Best-effort only (TOCTOU; on
    // tmpfs this reports RAM). resident_bytes() (arena + slot index) is the REAL
    // resident figure about to drain -- a conservative over-estimate of the run size.
    const uint64_t resident = resident_bytes();
    const uint64_t avail = temp_dir_available_bytes(dir);
    if (avail < resident) {
        return Status::IoError("spimi: insufficient temp space in '" + dir + "' to spill ~" +
                               std::to_string(resident) + " B (~" + std::to_string(avail) +
                               " B free); set SNII_TEMP_DIR/TMPDIR to a larger disk");
    }
    const std::string path = MakeRunPath(dir);
    RunWriter w;
    SNII_RETURN_IF_ERROR(w.open(path));
    run_paths_.push_back(path); // tracked for cleanup even if a later step fails
    SNII_RETURN_IF_ERROR(drain_to_writer(&w));
    // drain emptied touched_ids_ and freed each term's arrays; terms_/present_ keep
    // their (vocab-sized) capacity so the next fill reuses the dense slots with no
    // re-allocation. present_ is already all-zero after release_term per id.
    return w.close();
}

Status SpimiTermBuffer::merge_runs(const std::function<void(TermPostings&&)>& fn,
                                   bool allow_stream_positions) {
    // Flush whatever is still resident as one final sorted run so the k-way merge
    // sees a uniform set of run files (and never holds two term sources at once).
    if (!touched_ids_.empty()) {
        Status s = spill_to_run();
        if (!s.ok() && spill_status_.ok()) spill_status_ = s;
    }
    if (!spill_status_.ok()) return spill_status_; // a spill or add_token error; emit nothing
    // All terms are now spilled; the merge reads runs and never touches the
    // accumulators. Free the pool + the vocab-sized slot index so the merge phase
    // holds none of the input-side arrays resident -- keeps spill-mode peak RSS
    // down. malloc_trim(0) returns the freed glibc arenas to the OS so the peak RSS
    // measurement reflects the merge transient, not retained input-phase chunks.
    std::vector<Term>().swap(slots_);
    std::vector<uint32_t>().swap(free_slots_);
    std::vector<uint32_t>().swap(slot_of_);
    TrimMalloc();
    // pool_ was already reset by the final spill_to_run -> drain_to_writer (reported
    // there); this swap frees slot_of_, so report the remaining negative now. After a
    // full spilled drain reported_resident_ returns to 0 (no leak).
    report_arena_delta();
    Status s = MergeRuns(run_paths_, vocab(), has_positions_, fn, allow_stream_positions);
    // The merge churns one large coalesced TermPostings per term (the widest term's
    // arrays are tens of MiB) plus per-run reader windows; on completion glibc
    // retains those freed chunks in its arenas. Trim again so the post-merge resident
    // set (and thus the process peak high-water if a later phase allocates) reflects
    // only live state, not merge-transient retention.
    TrimMalloc();
    return s;
}

Status SpimiTermBuffer::for_each_term_sorted(const std::function<void(TermPostings&&)>& fn) {
    // Single-drain contract: a second call would re-merge the (still-present) run
    // files and re-emit every term, or emit nothing in the in-memory path. Return
    // an error and emit NOTHING rather than produce a wrong second stream.
    if (drained_) {
        return Status::Internal("spimi: already drained (single-drain contract)");
    }
    drained_ = true;
    // The callback is invoked synchronously while the arena is resident, so large
    // sorted terms may stream positions via pos_pump (peak-RSS win for the writer).
    if (run_paths_.empty() && spill_status_.ok()) {
        return drain_sorted(fn, /*allow_stream_positions=*/true); // pure in-memory path
    }
    // Spilled path (or add_token latched a validation error): the merge may STREAM
    // a wide term's positions via pos_pump (fn consumes each term synchronously
    // while the run readers stay parked). merge_runs returns the I/O status
    // directly; add_token validation errors surface via spill_status_ inside it.
    return merge_runs(fn, /*allow_stream_positions=*/true);
}

std::vector<TermPostings> SpimiTermBuffer::finalize_sorted() {
    std::vector<TermPostings> out;
    // Single-drain contract (mirrors for_each_term_sorted): a second drain (including
    // a finalize_sorted after a for_each_term_sorted, or vice versa) would re-emit or
    // emit nothing. Latch an error and return EMPTY rather than a wrong result.
    if (drained_) {
        if (spill_status_.ok()) {
            spill_status_ = Status::Internal("spimi: already drained (single-drain contract)");
        }
        return out;
    }
    drained_ = true;
    out.reserve(touched_ids_.size());
    // RETAINS each TermPostings past the drain, so positions must be MATERIALIZED
    // (a streamed pos_pump would reference the arena, freed when the drain ends).
    if (run_paths_.empty() && spill_status_.ok()) {
        Status s = drain_sorted([&out](TermPostings&& tp) { out.push_back(std::move(tp)); },
                                /*allow_stream_positions=*/false);
        if (!s.ok() && spill_status_.ok()) spill_status_ = s;
    } else {
        // RETAINS each TermPostings past the merge, so positions MUST be materialized
        // (a streamed pos_pump would reference run readers freed when the merge ends).
        Status s = merge_runs([&out](TermPostings&& tp) { out.push_back(std::move(tp)); },
                              /*allow_stream_positions=*/false);
        if (!s.ok() && spill_status_.ok()) spill_status_ = s;
    }
    return out;
}

void SpimiTermBuffer::cleanup_runs() {
    for (const std::string& p : run_paths_) std::remove(p.c_str());
    run_paths_.clear();
}

} // namespace snii::writer
