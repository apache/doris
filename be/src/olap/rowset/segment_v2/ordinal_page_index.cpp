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

#include "olap/rowset/segment_v2/ordinal_page_index.h"

#include "common/logging.h"
#include "env/env.h"
#include "olap/fs/fs_util.h"
#include "olap/key_coder.h"
#include "olap/rowset/segment_v2/page_handle.h"
#include "olap/rowset/segment_v2/page_io.h"

namespace doris {
namespace segment_v2 {

void OrdinalIndexWriter::append_entry(ordinal_t ordinal, const PagePointer& data_pp) {
    std::string key;
    KeyCoderTraits<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>::full_encode_ascending(&ordinal, &key);
    _page_builder->add(key, data_pp);
    _last_pp = data_pp;
}

Status OrdinalIndexWriter::finish(fs::WritableBlock* wblock, ColumnIndexMetaPB* meta) {
    CHECK(_page_builder->count() > 0) << "no entry has been added, filepath=" << wblock->path_desc().filepath;
    meta->set_type(ORDINAL_INDEX);
    BTreeMetaPB* root_page_meta = meta->mutable_ordinal_index()->mutable_root_page();

    if (_page_builder->count() == 1) {
        // only one data page, no need to write index page
        root_page_meta->set_is_root_data_page(true);
        _last_pp.to_proto(root_page_meta->mutable_root_page());
    } else {
        OwnedSlice page_body;
        PageFooterPB page_footer;
        _page_builder->finish(&page_body, &page_footer);

        // write index page (currently it's not compressed)
        PagePointer pp;
        RETURN_IF_ERROR(PageIO::write_page(wblock, {page_body.slice()}, page_footer, &pp));

        root_page_meta->set_is_root_data_page(false);
        pp.to_proto(root_page_meta->mutable_root_page());
    }
    return Status::OK();
}

Status OrdinalIndexReader::load(bool use_page_cache, bool kept_in_memory) {
    if (_index_meta->root_page().is_root_data_page()) {
        // only one data page, no index page
        _num_pages = 1;
        _ordinals.push_back(0);
        _ordinals.push_back(_num_values);
        _pages.emplace_back(_index_meta->root_page().root_page());
        return Status::OK();
    }
    // need to read index page
    std::unique_ptr<fs::ReadableBlock> rblock;
    fs::BlockManager* block_mgr = fs::fs_util::block_manager(_path_desc.storage_medium);
    RETURN_IF_ERROR(block_mgr->open_block(_path_desc, &rblock));

    PageReadOptions opts;
    opts.rblock = rblock.get();
    opts.page_pointer = PagePointer(_index_meta->root_page().root_page());
    opts.codec = nullptr; // ordinal index page uses NO_COMPRESSION right now
    OlapReaderStatistics tmp_stats;
    opts.stats = &tmp_stats;
    opts.use_page_cache = use_page_cache;
    opts.kept_in_memory = kept_in_memory;
    opts.type = INDEX_PAGE;

    // read index page
    PageHandle page_handle;
    Slice body;
    PageFooterPB footer;
    RETURN_IF_ERROR(PageIO::read_and_decompress_page(opts, &page_handle, &body, &footer));

    // parse and save all (ordinal, pp) from index page
    IndexPageReader reader;
    RETURN_IF_ERROR(reader.parse(body, footer.index_page_footer()));

    _num_pages = reader.count();
    _ordinals.resize(_num_pages + 1);
    _pages.resize(_num_pages);
    for (int i = 0; i < _num_pages; i++) {
        Slice key = reader.get_key(i);
        ordinal_t ordinal = 0;
        RETURN_IF_ERROR(KeyCoderTraits<OLAP_FIELD_TYPE_UNSIGNED_BIGINT>::decode_ascending(
                &key, sizeof(ordinal_t), (uint8_t*)&ordinal, nullptr));

        _ordinals[i] = ordinal;
        _pages[i] = reader.get_value(i);
    }
    _ordinals[_num_pages] = _num_values;
    return Status::OK();
}

OrdinalPageIndexIterator OrdinalIndexReader::seek_at_or_before(ordinal_t ordinal) {
    int32_t left = 0;
    int32_t right = _num_pages - 1;
    while (left < right) {
        int32_t mid = (left + right + 1) / 2;

        if (_ordinals[mid] < ordinal) {
            left = mid;
        } else if (_ordinals[mid] > ordinal) {
            right = mid - 1;
        } else {
            left = mid;
            break;
        }
    }
    if (_ordinals[left] > ordinal) {
        return OrdinalPageIndexIterator(this, _num_pages);
    }
    return OrdinalPageIndexIterator(this, left);
}

} // namespace segment_v2
} // namespace doris
