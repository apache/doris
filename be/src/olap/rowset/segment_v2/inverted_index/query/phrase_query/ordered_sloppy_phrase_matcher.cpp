#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/ordered_sloppy_phrase_matcher.h"

namespace doris::segment_v2::inverted_index {

OrderedSloppyPhraseMatcher::OrderedSloppyPhraseMatcher(std::vector<PostingsAndPosition> postings,
                                                       int32_t slop)
        : _allowed_slop(slop), _postings(std::move(postings)) {}

void OrderedSloppyPhraseMatcher::reset() {
    for (PostingsAndPosition& posting : _postings) {
        posting._freq = posting._postings.freq();
        posting._pos = -1;
        posting._upTo = 0;
    }
}

bool OrderedSloppyPhraseMatcher::next_match() {
    PostingsAndPosition* prev_posting = _postings.data();
    while (prev_posting->_upTo < prev_posting->_freq) {
        prev_posting->_pos = prev_posting->_postings.nextPosition();
        prev_posting->_upTo += 1;
        if (stretch_to_order(prev_posting) && _match_width <= _allowed_slop) {
            return true;
        }
    }
    return false;
}

bool OrderedSloppyPhraseMatcher::stretch_to_order(PostingsAndPosition* prev_posting) {
    _match_width = 0;
    for (size_t i = 1; i < _postings.size(); i++) {
        PostingsAndPosition& posting = _postings[i];
        if (!advance_position(posting, prev_posting->_pos + 1)) {
            return false;
        }
        _match_width += (posting._pos - (prev_posting->_pos + 1));
        prev_posting = &posting;
    }
    return true;
}

bool OrderedSloppyPhraseMatcher::advance_position(PostingsAndPosition& posting, int32_t target) {
    while (posting._pos < target) {
        if (posting._upTo == posting._freq) {
            return false;
        } else {
            posting._pos = posting._postings.nextPosition();
            posting._upTo += 1;
        }
    }
    return true;
}

} // namespace doris::segment_v2::inverted_index