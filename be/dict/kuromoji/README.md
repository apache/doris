# Kuromoji (Japanese) dictionary

This directory holds the compiled IPADIC dictionary consumed at runtime by the
`kuromoji` inverted-index analyzer (`KuromojiAnalyzer` → `KuromojiDictionary`):

- `system.bin`  — surface→word Darts trie + word entries + feature blob
- `matrix.bin`  — connection-cost matrix (1316×1316)
- `chardef.bin` — character-category map + per-category flags
- `unkdict.bin` — unknown-word entries per category

These `*.bin` files are **generated** (not committed; see `.gitignore`). The
runtime resolves them at `${inverted_index_dict_path}/kuromoji`
(default `${DORIS_HOME}/dict/kuromoji`); `be/CMakeLists.txt` installs this
directory into the BE package.

## How it's (re)generated

Source: the UTF-8 IPADIC from <https://github.com/lindera/mecab-ipadic>
(tag `2.7.0-20250920`) — the original `mecab-ipadic-2.7.0-20070801` lexicon
converted to UTF-8 (license: NAIST-2003, see `dist/licenses/LICENSE-ipadic.txt`).

Automated, two steps:

```bash
# 1. thirdparty fetches + stages the UTF-8 IPADIC source into
#    ${DORIS_THIRDPARTY}/installed/share/mecab-ipadic-2.7.0-20250920
sh thirdparty/build-thirdparty.sh mecab_ipadic

# 2. the CMake target builds the offline compiler and produces the *.bin here
ninja -C be/ut_build_RELEASE kuromoji_dict
```

CI/release should run `ninja kuromoji_dict` before packaging; the BE `install`
rule then ships this directory. Override the source dir with
`-DKUROMOJI_IPADIC_SRC=<path>` at CMake configure time. (The tool can also be
run directly: `kuromoji_build_dict <utf8_ipadic_src_dir> be/dict/kuromoji`.)
