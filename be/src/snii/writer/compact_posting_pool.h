#pragma once

#include <cstddef>
#include <cstdint>
#include <vector>

namespace snii::writer {

// SEGMENTED BYTE ARENA with per-term SLICED runs (a ByteBlockPool, after Lucene).
//
// WHY: the SPIMI accumulator's bulk memory is the per-term posting bytes. Backing
// each term with its own std::vector<uint8_t> pays two taxes that dominate peak
// RSS at scale: (1) geometric-growth doubling slack (~1.17x of the live payload),
// and (2) a 24-32 B vector/struct header per term (hundreds of thousands of
// terms). This pool removes both: all term bytes live in a few large fixed-size
// blocks (so slack is ~one block, amortized to ~1.05x), and a term needs only two
// 32-bit cursors of live state (chain head for reads + write head for appends).
//
// HOW (slices): a term's bytes are not stored contiguously. They live in a chain
// of SLICES of geometrically growing payload capacity (the kSliceSizes schedule:
// 4, 8, 16, ... bytes of payload). Each slice is laid out as
//   [ payload bytes ... ][ 4-byte forward pointer ]
// The forward pointer holds the absolute offset of the next slice's first payload
// byte (0 while the slice is still the tail of the chain). When a slice's payload
// region fills, the writer allocates a larger slice, stores its head into the old
// slice's 4 pointer bytes, and keeps appending. A reader walks the chain by
// reading payload bytes until a slice boundary, then following the pointer.
//
// Both writer and reader recompute each slice's capacity from the chain's slice
// INDEX (0, 1, 2, ...) via the deterministic schedule, so neither needs to store
// per-slice sizes. The writer carries the current slice's end offset in its
// SliceWriter handle; the reader recomputes capacities as it advances.
//
// Offsets are GLOBAL absolute byte indices into the logical concatenation of all
// blocks: offset = block_index * kBlockSize + byte_in_block. kBlockSize is a power
// of two, so offset -> (block, byte) is a shift/mask.
class CompactPostingPool {
public:
    // Block size (power of two). 32 KiB blocks keep per-block tail waste tiny (it
    // matters at the smaller 1M scale where the whole arena is only tens of MiB) and
    // bound the outer vector<block> header cost; at the 5M scale a few thousand
    // blocks is still cheap. Empirically the lowest peak across both scales.
    static constexpr uint32_t kBlockShift = 15;
    static constexpr uint32_t kBlockSize = 1u << kBlockShift; // 32 KiB
    static constexpr uint32_t kBlockMask = kBlockSize - 1;

    // Per-slice forward-pointer width (absolute uint32 next-slice offset).
    static constexpr uint32_t kPtrBytes = 4;

    // Geometric slice payload-capacity schedule and the level transition. Level i
    // slices hold kSliceSizes[i] payload bytes; on overflow the chain advances to
    // kNextLevel[i] (capping at the largest level). A GENTLE (~1.5x) many-level
    // schedule starting small minimizes the over-allocated final slice (the
    // dominant arena overhead) while keeping the per-slice forward-pointer count
    // bounded for high-df chains.
    static constexpr int kLevelCount = 16;

    CompactPostingPool();

    CompactPostingPool(const CompactPostingPool&) = delete;
    CompactPostingPool& operator=(const CompactPostingPool&) = delete;

    // Payload capacity (bytes) of a fresh level-0 slice. Exposed for tests that need
    // to fill exactly one slice without hardcoding the schedule.
    static uint32_t kSliceSizes_level0();

    // Payload capacity of the slice at `level`, and the level a chain advances to when
    // that slice overflows. Exposed (like kSliceSizes_level0) so tests can simulate the
    // arena's bump allocator exactly -- e.g. to construct an EXACT block-boundary fill --
    // without hardcoding the private schedule. `level` must be in [0, kLevelCount).
    static uint32_t kSliceSize_at(int level);
    static uint8_t kNextLevel_at(int level);

    // Live append handle for one term's chain. POD, 8 bytes: the absolute write
    // cursor and the absolute end of the current slice's payload region. The chain's
    // current slice LEVEL is kept by the caller (a uint8, packed alongside its other
    // flags) so this handle stays 8 bytes -- shaving the per-term accumulator. `head`
    // (the chain's first payload offset) is also stored by the CALLER (the read entry
    // point); start_chain returns it.
    struct SliceWriter {
        uint32_t cur = 0;       // next byte to write (absolute)
        uint32_t slice_end = 0; // one-past-last payload byte of the current slice
    };

    // Begins a fresh chain, initializing `w` to its first (level-0) slice and
    // *level to 0, and returns the chain head (absolute first payload offset).
    uint32_t start_chain(SliceWriter* w, uint8_t* level);

    // Appends one payload byte to the chain described by `w` / `*level`, growing the
    // chain with a new linked slice (and advancing *level) when the current slice's
    // payload region is exhausted.
    void append_byte(SliceWriter* w, uint8_t* level, uint8_t value);

    // Total live payload bytes ever written across all chains (excludes slice
    // forward-pointer overhead). Drives the spill-threshold estimate only.
    uint64_t payload_bytes() const { return payload_bytes_; }

    // Bytes the arena currently occupies (block_count * kBlockSize). The pool
    // addresses bytes with a uint32 offset (next_offset_), so the arena MUST stay
    // below 4 GiB or alloc_run wraps and silently aliases block 0. The accumulator
    // watches this to force a safety spill before the wrap; alloc_run also enforces it
    // directly (throws std::overflow_error on a would-be wrap) so a direct user of the
    // pool fails loudly rather than silently corrupting.
    // Hard invariant: a single CompactPostingPool never exceeds UINT32_MAX bytes.
    uint64_t arena_bytes() const { return static_cast<uint64_t>(blocks_.size()) << kBlockShift; }

    // Releases ALL blocks back to the OS. Called after the accumulator is fully
    // drained (or before a spill's next fill) so no input-side bytes stay resident.
    void reset();

    // ---- Reader ----------------------------------------------------------------
    // Forward cursor over one term's chain, yielding its payload bytes in write
    // order by walking the slice forward pointers.
    //
    // CONTRACT of the `budget` ctor argument (single, unambiguous meaning):
    //   `budget` is an UPPER BOUND on the number of bytes this cursor may yield. It
    //   is NOT required to equal the exact payload length: passing the exact length
    //   is fine, and so is passing any value >= it (the production caller passes the
    //   chain's write-head offset, which always bounds the payload from above). The
    //   cursor is SELF-TERMINATING: once it walks off the last written byte it sees
    //   the tail slice's zero forward pointer and stops, regardless of how much
    //   budget remains. So an over-large budget can never make next() read past the
    //   chain (no aliasing of block 0, no off-chain access) -- the budget is purely a
    //   secondary cap. has_next() is therefore a reliable "more bytes remain"
    //   predicate for ANY budget >= the true length: it becomes false at the smaller
    //   of (budget exhausted, chain tail reached).
    class Cursor {
    public:
        Cursor(const CompactPostingPool* pool, uint32_t head, uint64_t budget);

        // True while the cursor can still yield a REAL payload byte: the budget is not
        // spent AND the cursor has not reached the chain tail. It peeks the tail forward
        // pointer at a slice boundary so it never reports a phantom trailing byte, making
        // has_next()/next() a safe loop for any budget >= the true payload length.
        bool has_next() const;
        // Yields the next payload byte. Returns 0 (and yields no more) once the chain
        // tail is reached or the budget is spent -- never reads past the chain.
        uint8_t next();

    private:
        const CompactPostingPool* pool_;
        uint32_t cur_;       // absolute read cursor
        uint32_t slice_end_; // one-past-last payload byte of the current slice
        uint32_t level_;     // current slice level
        uint64_t budget_;    // remaining byte budget (upper bound on bytes to yield)
    };

    // Builds a cursor over the chain at `head`. `budget` is an UPPER BOUND on bytes to
    // read (see Cursor's contract): the exact payload length or anything larger. The
    // production caller passes the write-head offset, which always bounds the payload
    // from above; the cursor self-terminates at the chain tail regardless.
    Cursor cursor(uint32_t head, uint64_t budget) const { return Cursor(this, head, budget); }

private:
    static const uint32_t kSliceSizes[kLevelCount];
    static const uint8_t kNextLevel[kLevelCount];

    uint8_t* at(uint32_t off) { return &blocks_[off >> kBlockShift][off & kBlockMask]; }
    const uint8_t* at(uint32_t off) const { return &blocks_[off >> kBlockShift][off & kBlockMask]; }

    // Reads/writes the 4-byte forward pointer at the END of a slice whose payload
    // region ends at `slice_end` (pointer occupies [slice_end, slice_end+4)).
    uint32_t read_ptr(uint32_t slice_end) const;
    void write_ptr(uint32_t slice_end, uint32_t next_head);

    // Reserves `bytes` contiguous bytes from the arena tail (a fresh block if the
    // current tail cannot hold them) and returns the first reserved absolute offset.
    // `bytes` must be <= kBlockSize.
    uint32_t alloc_run(uint32_t bytes);

    // Allocates a slice at `level` (payload region + 4 pointer bytes), zeroes its
    // forward pointer, and returns the first payload offset; sets *slice_end.
    uint32_t alloc_slice(int level, uint32_t* slice_end);

    std::vector<std::vector<uint8_t>> blocks_; // fixed kBlockSize blocks
    uint32_t next_offset_ = 0;                 // global bump pointer (absolute) into the tail block
    uint64_t payload_bytes_ = 0;
};

} // namespace snii::writer
