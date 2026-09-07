<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

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

A normal BE build (`sh build.sh`) generates these `*.bin` automatically: the
`kuromoji_dict` target is part of `ALL` and the `install` rule then ships this
directory. The target is defined only for real (`MAKE_TEST=OFF`) builds, not for
the unit-test tree.

To regenerate manually:

```bash
# 1. thirdparty fetches + stages the UTF-8 IPADIC source into
#    ${DORIS_THIRDPARTY}/installed/share/mecab-ipadic-2.7.0-20250920
sh thirdparty/build-thirdparty.sh mecab_ipadic

# 2. run the target in a real (non-test) build tree, e.g. the one sh build.sh
#    creates under be/build_<BUILD_TYPE> (build_Release by default)
ninja -C be/build_Release kuromoji_dict
```

Override the source dir with `-DKUROMOJI_IPADIC_SRC=<path>` at CMake configure
time. (The tool can also be run directly:
`kuromoji_build_dict <utf8_ipadic_src_dir> be/dict/kuromoji`.)
