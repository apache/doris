#include "snii/query/internal/docid_posting_reader.h"

#include <limits>
#include <utility>

#include "snii/common/slice.h"
#include "snii/format/dict_entry.h"
#include "snii/format/frq_pod.h"
#include "snii/format/frq_prelude.h"
#include "snii/io/batch_range_fetcher.h"
#include "snii/reader/windowed_posting.h"

namespace snii::query::internal {

using snii::format::DictEntry;
using snii::format::DictEntryEnc;
using snii::format::DictEntryKind;
using snii::format::FrqPreludeReader;
using snii::format::WindowMeta;
using snii::reader::LogicalIndexReader;

namespace {

Status decode_flat_docs(const DictEntry& entry, Slice dd_region, std::vector<uint32_t>* docids) {
    return snii::format::decode_dd_region(dd_region, entry.dd_meta,
                                          /*win_base=*/0, docids);
}

Status decode_inline_docs(const DictEntry& entry, std::vector<uint32_t>* docids) {
    if (entry.dd_meta.disk_len > entry.frq_bytes.size()) {
        return Status::Corruption("docid_posting_reader: inline dd region exceeds frq bytes");
    }
    return decode_flat_docs(
            entry, Slice(entry.frq_bytes.data(), static_cast<size_t>(entry.dd_meta.disk_len)),
            docids);
}

Status slim_docs_fetch_len(const DictEntry& entry, uint64_t win_len, uint64_t* out) {
    if (entry.frq_docs_len > win_len) {
        return Status::Corruption("docid_posting_reader: slim frq_docs_len exceeds frq window");
    }
    *out = entry.frq_docs_len > 0 ? entry.frq_docs_len : win_len;
    return Status::OK();
}

Status add_u64(uint64_t lhs, uint64_t rhs, const char* message, uint64_t* out) {
    if (rhs > std::numeric_limits<uint64_t>::max() - lhs) {
        return Status::Corruption(message);
    }
    *out = lhs + rhs;
    return Status::OK();
}

Status prelude_abs(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
                   uint64_t* out) {
    uint64_t with_base = 0;
    SNII_RETURN_IF_ERROR(add_u64(idx.section_refs().posting_region.offset, frq_base,
                                 "docid_posting_reader: prelude offset overflow", &with_base));
    return add_u64(with_base, entry.frq_off_delta, "docid_posting_reader: prelude offset overflow",
                   out);
}

Status validate_windowed_docs_prefix(const DictEntry& entry) {
    if (entry.prelude_len == 0) {
        return Status::Corruption("docid_posting_reader: windowed entry has no prelude");
    }
    if (entry.prelude_len > entry.frq_docs_len) {
        return Status::Corruption("docid_posting_reader: prelude_len exceeds docs prefix");
    }
    if (entry.frq_docs_len > entry.frq_len) {
        return Status::Corruption("docid_posting_reader: docs prefix exceeds frq_len");
    }
    return Status::OK();
}

struct FlatPlan {
    size_t out_index = 0;
    const DictEntry* entry = nullptr;
    size_t handle = 0;
};

struct WindowPlan {
    size_t out_index = 0;
    const ResolvedDocidPosting* posting = nullptr;
    size_t prefix_handle = 0;
};

Status plan_flat_docs(const LogicalIndexReader& idx, const ResolvedDocidPosting& posting,
                      snii::io::BatchRangeFetcher* fetcher, FlatPlan* plan) {
    uint64_t win_abs = 0;
    uint64_t win_len = 0;
    SNII_RETURN_IF_ERROR(
            idx.resolve_frq_window(posting.entry, posting.frq_base, &win_abs, &win_len));
    uint64_t docs_len = 0;
    SNII_RETURN_IF_ERROR(slim_docs_fetch_len(posting.entry, win_len, &docs_len));
    plan->handle = fetcher->add(win_abs, docs_len);
    return Status::OK();
}

Status plan_window_prefix(const LogicalIndexReader& idx, WindowPlan* plan,
                          snii::io::BatchRangeFetcher* fetcher) {
    const ResolvedDocidPosting& posting = *plan->posting;
    SNII_RETURN_IF_ERROR(validate_windowed_docs_prefix(posting.entry));
    uint64_t abs = 0;
    SNII_RETURN_IF_ERROR(prelude_abs(idx, posting.entry, posting.frq_base, &abs));
    plan->prefix_handle = fetcher->add(abs, posting.entry.frq_docs_len);
    return Status::OK();
}

Status window_dd_slice(Slice dd_block, const WindowMeta& meta, Slice* out) {
    if (meta.dd_off > dd_block.size() || meta.dd_disk_len > dd_block.size() - meta.dd_off) {
        return Status::Corruption("docid_posting_reader: window dd range out of prefix");
    }
    *out = dd_block.subslice(static_cast<size_t>(meta.dd_off),
                             static_cast<size_t>(meta.dd_disk_len));
    return Status::OK();
}

Status decode_flat_plan(const snii::io::BatchRangeFetcher& fetcher, const FlatPlan& plan,
                        std::vector<uint32_t>* out) {
    return decode_flat_docs(*plan.entry, fetcher.get(plan.handle), out);
}

Status decode_window_prefix_plan(const snii::io::BatchRangeFetcher& fetcher, const WindowPlan& plan,
                                 std::vector<uint32_t>* out) {
    const DictEntry& entry = plan.posting->entry;
    const Slice prefix = fetcher.get(plan.prefix_handle);
    if (entry.prelude_len > prefix.size()) {
        return Status::Corruption("docid_posting_reader: short docs prefix");
    }
    const size_t prelude_len = static_cast<size_t>(entry.prelude_len);
    FrqPreludeReader prelude;
    SNII_RETURN_IF_ERROR(FrqPreludeReader::open(prefix.subslice(0, prelude_len), &prelude));
    const uint64_t dd_block_len = prelude.dd_block_len();
    if (dd_block_len > static_cast<uint64_t>(std::numeric_limits<size_t>::max()) - prelude_len) {
        return Status::Corruption("docid_posting_reader: docs prefix length overflow");
    }
    const size_t expected_prefix_len = prelude_len + static_cast<size_t>(dd_block_len);
    if (prefix.size() != expected_prefix_len) {
        return Status::Corruption("docid_posting_reader: docs prefix length mismatch");
    }
    const Slice dd_block = prefix.subslice(prelude_len, prefix.size() - prelude_len);
    for (uint32_t w = 0; w < prelude.n_windows(); ++w) {
        WindowMeta meta;
        Slice dd_region;
        SNII_RETURN_IF_ERROR(prelude.window(w, &meta));
        SNII_RETURN_IF_ERROR(window_dd_slice(dd_block, meta, &dd_region));
        std::vector<uint32_t> docs;
        std::vector<uint32_t> freqs;
        std::vector<std::vector<uint32_t>> positions;
        SNII_RETURN_IF_ERROR(snii::reader::decode_window_slices(
                meta, dd_region, Slice(), Slice(), /*want_positions=*/false,
                /*want_freq=*/false, &docs, &freqs, &positions));
        out->insert(out->end(), docs.begin(), docs.end());
    }
    return Status::OK();
}

} // namespace

Status read_docid_posting(const LogicalIndexReader& idx, const DictEntry& entry, uint64_t frq_base,
                          uint64_t prx_base, std::vector<uint32_t>* docids) {
    if (docids == nullptr) {
        return Status::InvalidArgument("docid_posting_reader: null out");
    }
    std::vector<std::vector<uint32_t>> batched;
    SNII_RETURN_IF_ERROR(read_docid_postings_batched(
            idx, {ResolvedDocidPosting {entry, frq_base, prx_base}}, &batched));
    *docids = std::move(batched.front());
    return Status::OK();
}

Status read_docid_postings_batched(const LogicalIndexReader& idx,
                                   const std::vector<ResolvedDocidPosting>& postings,
                                   std::vector<std::vector<uint32_t>>* docids) {
    if (docids == nullptr) {
        return Status::InvalidArgument("docid_posting_reader: null batched out");
    }
    docids->clear();
    docids->resize(postings.size());

    std::vector<FlatPlan> flat_plans;
    std::vector<WindowPlan> window_plans;
    snii::io::BatchRangeFetcher docs_fetcher(idx.reader());

    for (size_t i = 0; i < postings.size(); ++i) {
        const ResolvedDocidPosting& posting = postings[i];
        if (posting.entry.kind == DictEntryKind::kInline) {
            SNII_RETURN_IF_ERROR(decode_inline_docs(posting.entry, &(*docids)[i]));
            continue;
        }
        if (posting.entry.enc == DictEntryEnc::kWindowed) {
            WindowPlan plan;
            plan.out_index = i;
            plan.posting = &posting;
            SNII_RETURN_IF_ERROR(plan_window_prefix(idx, &plan, &docs_fetcher));
            window_plans.push_back(std::move(plan));
            continue;
        }
        FlatPlan plan;
        plan.out_index = i;
        plan.entry = &posting.entry;
        flat_plans.push_back(plan);
    }

    for (FlatPlan& plan : flat_plans) {
        const ResolvedDocidPosting& posting = postings[plan.out_index];
        SNII_RETURN_IF_ERROR(plan_flat_docs(idx, posting, &docs_fetcher, &plan));
    }
    if (docs_fetcher.pending() > 0) SNII_RETURN_IF_ERROR(docs_fetcher.fetch());

    for (const FlatPlan& plan : flat_plans) {
        SNII_RETURN_IF_ERROR(decode_flat_plan(docs_fetcher, plan, &(*docids)[plan.out_index]));
    }
    for (const WindowPlan& plan : window_plans) {
        SNII_RETURN_IF_ERROR(
                decode_window_prefix_plan(docs_fetcher, plan, &(*docids)[plan.out_index]));
    }
    return Status::OK();
}

} // namespace snii::query::internal
