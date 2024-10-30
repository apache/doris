#include "olap/rowset/segment_v2/inverted_index/query/phrase_query/exact_phrase_matcher.h"

namespace doris::segment_v2::inverted_index {

ExactPhraseMatcher::ExactPhraseMatcher(std::vector<PostingsAndPosition> postings)
        : _postings(std::move(postings)) {}

void ExactPhraseMatcher::reset() {
    for (PostingsAndPosition& posting : _postings) {
        posting._freq = posting._postings.freq();
        posting._pos = -1;
        posting._upTo = 0;
    }
}

bool ExactPhraseMatcher::next_match() {
    PostingsAndPosition& lead = _postings[0];
    if (lead._upTo < lead._freq) {
        lead._pos = lead._postings.nextPosition();
        lead._upTo += 1;
    } else {
        return false;
    }

    while (true) {
        int32_t phrasePos = lead._pos - lead._offset;

        bool advance_head = false;
        for (size_t j = 1; j < _postings.size(); ++j) {
            PostingsAndPosition& posting = _postings[j];
            int32_t expectedPos = phrasePos + posting._offset;
            // advance up to the same position as the lead
            if (!advance_position(posting, expectedPos)) {
                return false;
            }

            if (posting._pos != expectedPos) { // we advanced too far
                if (advance_position(lead, posting._pos - posting._offset + lead._offset)) {
                    advance_head = true;
                    break;
                } else {
                    return false;
                }
            }
        }
        if (advance_head) {
            continue;
        }

        return true;
    }

    return false;
}

bool ExactPhraseMatcher::advance_position(PostingsAndPosition& posting, int32_t target) {
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