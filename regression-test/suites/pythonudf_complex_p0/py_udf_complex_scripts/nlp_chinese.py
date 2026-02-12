# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Chinese NLP Processing using jieba

Dependencies:
- jieba (Chinese text segmentation)

This module provides Chinese text processing capabilities that
are not available in Doris native functions.
"""

import json

# Import jieba for Chinese word segmentation
try:
    import jieba
    import jieba.analyse
    import jieba.posseg as pseg
    JIEBA_AVAILABLE = True
except ImportError:
    JIEBA_AVAILABLE = False


# ==================== Chinese Word Segmentation ====================

def chinese_word_segment(text, mode='accurate'):
    """
    Segment Chinese text into words using jieba

    Args:
        text: Chinese text to segment
        mode: Segmentation mode
            - 'accurate': Accurate mode (default), best for text analysis
            - 'full': Full mode, scans all possible words
            - 'search': Search engine mode, based on accurate with long words further segmented

    Returns:
        Space-separated words
    """
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None:
        return None

    try:
        text = str(text)
        if mode == 'full':
            words = jieba.cut(text, cut_all=True)
        elif mode == 'search':
            words = jieba.cut_for_search(text)
        else:  # accurate
            words = jieba.cut(text, cut_all=False)

        return ' '.join(words)

    except Exception as e:
        return json.dumps({'error': str(e)})


def chinese_word_segment_json(text, mode='accurate'):
    """
    Segment Chinese text and return as JSON array

    Returns:
        JSON array of words
    """
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None:
        return None

    try:
        text = str(text)
        if mode == 'full':
            words = list(jieba.cut(text, cut_all=True))
        elif mode == 'search':
            words = list(jieba.cut_for_search(text))
        else:
            words = list(jieba.cut(text, cut_all=False))

        return json.dumps(words, ensure_ascii=False)

    except Exception as e:
        return json.dumps({'error': str(e)})


def chinese_word_count(text):
    """
    Count Chinese words in text

    Returns:
        Number of words after segmentation
    """
    if not JIEBA_AVAILABLE:
        return -1

    if text is None:
        return 0

    try:
        words = list(jieba.cut(str(text), cut_all=False))
        # Filter out whitespace and punctuation
        words = [w for w in words if w.strip() and not all(c in '，。！？、；：""''（）【】《》' for c in w)]
        return len(words)

    except Exception:
        return -1


# ==================== Keyword Extraction ====================

def extract_keywords_tfidf(text, top_k=5, with_weight=False):
    """
    Extract keywords using TF-IDF algorithm

    Args:
        text: Chinese text
        top_k: Number of keywords to extract
        with_weight: If True, return keywords with weights as JSON

    Returns:
        Space-separated keywords, or JSON array with weights
    """
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None:
        return None

    try:
        top_k = int(top_k) if top_k else 5

        if with_weight:
            keywords = jieba.analyse.extract_tags(str(text), topK=top_k, withWeight=True)
            result = [{'keyword': kw, 'weight': round(w, 4)} for kw, w in keywords]
            return json.dumps(result, ensure_ascii=False)
        else:
            keywords = jieba.analyse.extract_tags(str(text), topK=top_k, withWeight=False)
            return ' '.join(keywords)

    except Exception as e:
        return json.dumps({'error': str(e)})


def extract_keywords_textrank(text, top_k=5, with_weight=False):
    """
    Extract keywords using TextRank algorithm

    Args:
        text: Chinese text
        top_k: Number of keywords to extract
        with_weight: If True, return keywords with weights as JSON

    Returns:
        Space-separated keywords, or JSON array with weights
    """
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None:
        return None

    try:
        top_k = int(top_k) if top_k else 5

        if with_weight:
            keywords = jieba.analyse.textrank(str(text), topK=top_k, withWeight=True)
            result = [{'keyword': kw, 'weight': round(w, 4)} for kw, w in keywords]
            return json.dumps(result, ensure_ascii=False)
        else:
            keywords = jieba.analyse.textrank(str(text), topK=top_k, withWeight=False)
            return ' '.join(keywords)

    except Exception as e:
        return json.dumps({'error': str(e)})


# ==================== Part-of-Speech Tagging ====================

def pos_tagging(text):
    """
    Part-of-speech tagging for Chinese text

    Returns:
        JSON array of {word, pos} objects

    POS tags include:
        n: noun, v: verb, a: adjective, d: adverb,
        m: numeral, q: quantifier, r: pronoun, p: preposition,
        c: conjunction, u: auxiliary, x: punctuation, etc.
    """
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None:
        return None

    try:
        words = pseg.cut(str(text))
        result = [{'word': word, 'pos': flag} for word, flag in words]
        return json.dumps(result, ensure_ascii=False)

    except Exception as e:
        return json.dumps({'error': str(e)})


def extract_nouns(text):
    """Extract only nouns from Chinese text"""
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None:
        return None

    try:
        words = pseg.cut(str(text))
        nouns = [word for word, flag in words if flag.startswith('n')]
        return ' '.join(nouns)

    except Exception as e:
        return json.dumps({'error': str(e)})


def extract_verbs(text):
    """Extract only verbs from Chinese text"""
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None:
        return None

    try:
        words = pseg.cut(str(text))
        verbs = [word for word, flag in words if flag.startswith('v')]
        return ' '.join(verbs)

    except Exception as e:
        return json.dumps({'error': str(e)})


def extract_by_pos(text, pos_filter):
    """
    Extract words by specific part-of-speech

    Args:
        text: Chinese text
        pos_filter: POS tag prefix to filter (e.g., 'n' for nouns, 'v' for verbs, 'nr' for person names)

    Returns:
        Space-separated filtered words
    """
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None or pos_filter is None:
        return None

    try:
        words = pseg.cut(str(text))
        filtered = [word for word, flag in words if flag.startswith(str(pos_filter))]
        return ' '.join(filtered)

    except Exception as e:
        return json.dumps({'error': str(e)})


# ==================== Named Entity Recognition (Simple) ====================

def extract_person_names(text):
    """Extract person names from Chinese text (POS tag: nr)"""
    return extract_by_pos(text, 'nr')


def extract_place_names(text):
    """Extract place/location names from Chinese text (POS tag: ns)"""
    return extract_by_pos(text, 'ns')


def extract_org_names(text):
    """Extract organization names from Chinese text (POS tag: nt)"""
    return extract_by_pos(text, 'nt')


def extract_all_entities(text):
    """
    Extract all named entities (person, place, organization)

    Returns:
        JSON object with entity types as keys
    """
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None:
        return None

    try:
        words = list(pseg.cut(str(text)))

        persons = list(set([word for word, flag in words if flag == 'nr']))
        places = list(set([word for word, flag in words if flag == 'ns']))
        orgs = list(set([word for word, flag in words if flag == 'nt']))

        return json.dumps({
            'persons': sorted(persons),
            'places': sorted(places),
            'organizations': sorted(orgs)
        }, ensure_ascii=False)

    except Exception as e:
        return json.dumps({'error': str(e)})


# ==================== Custom Dictionary ====================

def add_custom_word(word, freq=None, tag=None):
    """
    Add a custom word to jieba dictionary

    Args:
        word: Word to add
        freq: Word frequency (optional)
        tag: POS tag (optional)

    Returns:
        'success' or error message
    """
    if not JIEBA_AVAILABLE:
        return 'error: jieba not installed'

    if word is None:
        return 'error: word is required'

    try:
        jieba.add_word(str(word), freq=freq, tag=tag)
        return 'success'

    except Exception as e:
        return f'error: {str(e)}'


def segment_with_custom_dict(text, custom_words_json):
    """
    Segment text after adding custom words

    Args:
        text: Text to segment
        custom_words_json: JSON array of custom words, e.g., '["机器学习", "深度学习"]'

    Returns:
        Space-separated words
    """
    if not JIEBA_AVAILABLE:
        return json.dumps({'error': 'jieba package not installed. Run: pip install jieba'})

    if text is None:
        return None

    try:
        # Add custom words
        if custom_words_json:
            custom_words = json.loads(custom_words_json)
            for word in custom_words:
                jieba.add_word(str(word))

        # Segment
        words = jieba.cut(str(text), cut_all=False)
        return ' '.join(words)

    except Exception as e:
        return json.dumps({'error': str(e)})


# ==================== Text Similarity (based on word overlap) ====================

def chinese_text_similarity(text1, text2):
    """
    Calculate similarity between two Chinese texts based on word overlap (Jaccard)

    Returns:
        Similarity score between 0 and 1
    """
    if not JIEBA_AVAILABLE:
        return -1.0

    if text1 is None or text2 is None:
        return 0.0

    try:
        words1 = set(jieba.cut(str(text1)))
        words2 = set(jieba.cut(str(text2)))

        # Remove punctuation and whitespace
        punct = set('，。！？、；：""''（）【】《》 \t\n')
        words1 = words1 - punct
        words2 = words2 - punct

        if not words1 or not words2:
            return 0.0

        intersection = len(words1 & words2)
        union = len(words1 | words2)

        return round(intersection / union, 4) if union > 0 else 0.0

    except Exception:
        return -1.0


# ==================== Sentence Tokenization ====================

def split_sentences(text):
    """
    Split Chinese text into sentences

    Returns:
        JSON array of sentences
    """
    if text is None:
        return None

    try:
        import re
        # Split by Chinese sentence-ending punctuation
        sentences = re.split(r'[。！？\n]+', str(text))
        sentences = [s.strip() for s in sentences if s.strip()]
        return json.dumps(sentences, ensure_ascii=False)

    except Exception as e:
        return json.dumps({'error': str(e)})


def sentence_count(text):
    """Count number of sentences in Chinese text"""
    if text is None:
        return 0

    try:
        import re
        sentences = re.split(r'[。！？\n]+', str(text))
        sentences = [s.strip() for s in sentences if s.strip()]
        return len(sentences)

    except Exception:
        return 0
