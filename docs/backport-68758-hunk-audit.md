# PR #68758 backport hunk audit for branch-4.2

Source: apache/doris#68758, squash commit `14fde1c1dc096f09c17af019c5e0cff3175da6c9`.
Target base: `origin/branch-4.2` at `9ffd818bec3`.

Each row below covers a source diff `@@` hunk. A file is collapsed to one row only when its post-pick content is byte-for-byte identical to the source post-image, or both trees delete it. `Ported (hunk post-image)` means every source addition is present in source order and every source removal absent, except lines that remain elsewhere in the source post-image. `Adapted` rows explain branch-specific API changes. The source and target use different storage and index APIs, so build and tests provide the behavior check.

| Source path | Source hunk | Disposition |
| --- | --- | --- |
| `.clang-format-ignore` | `@@ -11 +11 @@` | N-A: the listed Kuromoji source is absent on branch-4.2 |
| `.github/CODEOWNERS` | `@@ -28,2 +28 @@` | N-A: branch-4.2 retains its existing ownership rule |
| `.licenserc.yaml` | `@@ -129 +129 @@` | N-A: the listed Kuromoji source is absent on branch-4.2 |
| `LICENSE.txt` | `@@ -372 +372 @@` | N-A: the listed Kuromoji source is absent on branch-4.2 |
| `NOTICE.txt` | `@@ -78 +78 @@` | N-A: the listed Kuromoji source is absent on branch-4.2 |
| `be/benchmark/benchmark_gram_extractor.hpp` | `all hunks` | Ported (verbatim) |
| `be/src/cloud/cloud_schema_change_job.cpp` | `@@ -36 +36 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/cloud/cloud_tablet.cpp` | `@@ -54 +54 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/cloud/cloud_warm_up_manager.cpp` | `@@ -49 +49 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exec/operator/olap_scan_operator.cpp` | `@@ -375 +374,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exec/operator/olap_scan_operator.h` | `@@ -32 +32 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exec/operator/olap_scan_operator.h` | `@@ -163 +162,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exec/scan/olap_scanner.cpp` | `@@ -44,0 +45 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exec/scan/olap_scanner.cpp` | `@@ -57,2 +58,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exec/scan/olap_scanner.cpp` | `@@ -281 +282,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exec/scan/olap_scanner.cpp` | `@@ -891 +892,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/array/function_array_index.h` | `@@ -52 +52 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/function_multi_match.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/exprs/function/function_search.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/exprs/function/function_tokenize.cpp` | `@@ -39,2 +39,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/function_tokenize.h` | `all hunks` | Ported (verbatim) |
| `be/src/exprs/function/match.cpp` | `@@ -24,0 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/match.cpp` | `@@ -32 +33,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/match.cpp` | `@@ -661 +663,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/match.h` | `@@ -44 +44,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/match.h` | `@@ -46 +46,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/function_search.cpp` | `@@ -0,0 +1,658 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/function_search.h` | `@@ -42 +42 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/function_search.h` | `@@ -44,2 +44,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/function_search.h` | `@@ -104,25 +104,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/function_search.h` | `@@ -134 +112,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/function_search.h` | `@@ -136,5 +118,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/function_search_leaf.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/exprs/function/search/function_search_leaf.h` | `all hunks` | Ported (verbatim) |
| `be/src/exprs/function/search/search_lowering.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/exprs/function/search/search_lowering.h` | `all hunks` | Ported (verbatim) |
| `be/src/exprs/function/search/search_predicate_collector.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/exprs/function/search/search_predicate_collector.h` | `all hunks` | Ported (verbatim) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -20,0 +21,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -28 +31,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -31 +34 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -35,11 +38,11 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -52,2 +54,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -247 +248,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -273 +275,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -298,102 +301,13 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -401,8 +315,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -420,2 +333 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -428 +339,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -436,6 +347,9 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -442,0 +357 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -444,4 +359,15 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -449,2 +375 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -454 +379 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -459 +384,13 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -465,3 +402,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -767,0 +705 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.cpp` | `@@ -770 +708,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -38 +38 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -41,2 +41,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -62,0 +64 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -72,7 +73,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -86 +81 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -92 +87,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -96,7 +92 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -117,0 +108 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -122,3 +113,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -130,3 +121 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -134,3 +123,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -139,3 +127,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -147,0 +135,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -179,4 +172,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/function/search/variant_inverted_index_search.h` | `@@ -191,2 +185,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/vexpr.cpp` | `@@ -72 +72 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/vmatch_predicate.cpp` | `@@ -48 +48 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/vmatch_predicate.h` | `@@ -27 +27 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/exprs/vsearch.cpp` | `@@ -28 +28 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/load/channel/load_stream_writer.cpp` | `@@ -46 +46 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/load/delta_writer/delta_writer_v2.cpp` | `@@ -43 +43 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/runtime/exec_env_init.cpp` | `@@ -109 +109 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/runtime/index_policy/index_policy_mgr.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/runtime/index_policy/index_policy_mgr.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/CMakeLists.txt` | `@@ -62,8 +62,8 @@` | N-A: branch-4.2 does not use the source unity-build skip list |
| `be/src/storage/compaction/compaction.cpp` | `@@ -72,8 +72,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/analyzer_key_matcher.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/ann/ann_index_iterator.cpp` | `@@ -22,0 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_index_iterator.cpp` | `@@ -66 +67 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_index_reader.cpp` | `@@ -36,0 +37 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_index_reader.cpp` | `@@ -38 +38,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_index_reader.h` | `@@ -25,0 +26,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_index_reader.h` | `@@ -27,2 +28,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_index_writer.cpp` | `@@ -28 +28 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_index_writer.h` | `@@ -23 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_index_writer.h` | `@@ -43,0 +44 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_index_writer.h` | `@@ -45 +45,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/ann/ann_topn_runtime.cpp` | `@@ -42 +42 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_common.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/index_common.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/index_common_impl.h` | `@@ -34 +34 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_compound_reader.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_compound_reader.cpp` | `@@ -32 +32 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_compound_reader.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/index_file_reader.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/index_file_reader.h` | `@@ -23 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_file_reader.h` | `@@ -49,5 +49,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_file_writer.cpp` | `@@ -31,0 +32 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_file_writer.cpp` | `@@ -32,0 +34 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_file_writer.cpp` | `@@ -36,7 +38,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_file_writer.cpp` | `@@ -543,4 +544,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_file_writer.h` | `@@ -23 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_file_writer.h` | `@@ -44,0 +45,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_file_writer.h` | `@@ -46,7 +48,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_fs_directory.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/index_iterator.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/index_query_context.h` | `@@ -21 +20,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_query_context.h` | `@@ -23,2 +22,15 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_query_context.h` | `@@ -66,7 +78,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_query_context.h` | `@@ -90,7 +96,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_storage_format.cpp` | `@@ -21,2 +21,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_storage_format.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/index_storage_format_v1.cpp` | `@@ -22,2 +22,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_storage_format_v2.cpp` | `@@ -22,2 +22,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/index_writer.cpp` | `@@ -20,3 +20,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/abstract_analysis_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analysis_factory_mgr.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analysis_factory_mgr.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer.cpp` | `@@ -39,5 +39,9 @@` | Adapted: retained analyzer changes for available parsers; Kuromoji is absent |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer.cpp` | `@@ -62,0 +67,26 @@` | Adapted: retained analyzer changes for available parsers; Kuromoji is absent |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer.cpp` | `@@ -82,0 +113,12 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer.cpp` | `@@ -236 +278 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer.cpp` | `@@ -241 +283 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer.cpp` | `@@ -257,0 +300,58 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer_provider.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/analyzer_provider.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/basic/basic_analyzer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/custom_analyzer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/custom_analyzer.cpp` | `@@ -25,3 +25,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/custom_analyzer.h` | `@@ -24,7 +24,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/custom_analyzer_config.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/custom_analyzer_config.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/icu/icu_analyzer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/IKAnalyzer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/IKTokenizer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/IKTokenizer.cpp` | `@@ -25 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/IKTokenizer.cpp` | `@@ -104,2 +104,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/IKTokenizer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/AnalyzeContext.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/AnalyzeContext.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/CJKSegmenter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/CJKSegmenter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/CN_QuantifierSegmenter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/CN_QuantifierSegmenter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/CharacterUtil.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/IKArbitrator.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/IKArbitrator.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/IKSegmenter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/IKSegmenter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/ISegmenter.h` | `@@ -21 +21 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/LetterSegmenter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/LetterSegmenter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/Lexeme.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/Lexeme.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/LexemePath.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/LexemePath.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/QuickSortSet.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/QuickSortSet.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/SurrogatePairSegmenter.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/SurrogatePairSegmenter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/dic/DictSegment.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/dic/DictSegment.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/dic/Dictionary.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/ik/dic/Dictionary.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/KuromojiAnalyzer.h` | `@@ -25,2 +25,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/KuromojiTokenizer.cpp` | `@@ -18,3 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/KuromojiTokenizer.cpp` | `@@ -27 +25,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/KuromojiTokenizer.cpp` | `@@ -53,48 +52,5 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/KuromojiTokenizer.cpp` | `@@ -186 +142 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/KuromojiTokenizer.h` | `@@ -26,2 +26,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_dictionary.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_dictionary.h` | `@@ -28,2 +28,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_dictionary_builder.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_dictionary_builder.cpp` | `@@ -25 +25 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_dictionary_builder.h` | `@@ -27 +27 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_ipadic_parser.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/dict/kuromoji_ipadic_parser.h` | `@@ -25 +25 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_normalize.h` | `@@ -28 +28 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi.h` | `@@ -24,2 +24,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/src/storage/index/inverted/analysis/char_filter/char_filter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/char_filter/char_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/char_filter/char_replace_char_filter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/char_filter/char_replace_char_filter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/char_filter/char_replace_char_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/char_filter/empty_char_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/char_filter/icu_normalizer_char_filter.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/char_filter/icu_normalizer_char_filter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/char_filter/icu_normalizer_char_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/gram/gram_density.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/gram/gram_extractor.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/gram/gram_extractor.h` | `@@ -24 +24 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/gram/gram_family.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/gram/gram_family.cpp` | `@@ -21,3 +21,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/gram/gram_family.h` | `@@ -24 +24 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/gram/gram_scheme.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/inverted_index_parser.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/inverted_index_parser.cpp` | `@@ -25 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/inverted_index_parser.h` | `@@ -26 +26 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/normalizer/custom_normalizer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/normalizer/custom_normalizer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/normalizer/custom_normalizer_config.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/normalizer/custom_normalizer_config.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -198 +198 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -241 +241 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -314 +314 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -326 +326 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -345 +345 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -361 +361 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -384 +384 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -404 +404 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -464 +464 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -507 +507 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -521 +521 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -531 +531 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -577 +577 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -593 +593 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -613 +613 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -632 +632 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -670 +670 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -694 +694 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -696 +696 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -716 +716 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -728 +728 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -748 +748 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -764 +764 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -790 +790 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -813 +813 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -858 +858 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -870 +870 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -893 +893 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -913 +913 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -915 +915 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -978 +978 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1027 +1027 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1071 +1071 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1085 +1085 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1097 +1097 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1106 +1106 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1136 +1136 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1160 +1160 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1181 +1181 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1203 +1203 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1237 +1237 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1257 +1257 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1326 +1326 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1363 +1363 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1384 +1384 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1397 +1397 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1423 +1423 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1437 +1437 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1448 +1448 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1456 +1456 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1478 +1478 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1496 +1496 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1517 +1517 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.cpp` | `@@ -1537 +1537 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/ascii_folding_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/empty_token_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/icu_normalizer_filter.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/icu_normalizer_filter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/icu_normalizer_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/lower_case_filter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/lower_case_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/pinyin_filter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/pinyin_filter.cpp` | `@@ -26,4 +26,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/pinyin_filter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/pinyin_filter_factory.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/pinyin_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/token_filter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/token_filter_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/word_delimiter_filter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/word_delimiter_filter.cpp` | `@@ -27 +27 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/word_delimiter_filter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/token_filter/word_delimiter_filter_factory.h` | `@@ -22,3 +22,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_filter/word_delimiter_iterator.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/token_stream.h` | `@@ -33 +33 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/basic/basic_tokenizer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/basic/basic_tokenizer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/basic/basic_tokenizer_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/char/char_group_tokenizer_factory.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/char/char_group_tokenizer_factory.cpp` | `@@ -23,2 +23,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/char/char_group_tokenizer_factory.h` | `@@ -20 +20 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/char/char_tokenizer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/char/char_tokenizer.h` | `@@ -23 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/empty/empty_tokenizer_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/break_iterator_wrapper.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/break_iterator_wrapper.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/composite_break_iterator.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/composite_break_iterator.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/default_icu_tokenizer_config.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/default_icu_tokenizer_config.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer_config.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/icu_tokenizer_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/script_iterator.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/icu/script_iterator.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/ik/ik_tokenizer_factory.h` | `@@ -21,3 +21,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/keyword/keyword_tokenizer.h` | `@@ -20 +20 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/keyword/keyword_tokenizer_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/ngram/edge_ngram_tokenizer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/ngram/edge_ngram_tokenizer_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/ngram/gram_tokenizer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/ngram/gram_tokenizer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/ngram/ngram_tokenizer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/ngram/ngram_tokenizer.h` | `@@ -23 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/ngram/ngram_tokenizer_factory.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/ngram/ngram_tokenizer_factory.h` | `@@ -23,6 +23,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/chinese_util.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_alphabet_tokenizer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_format.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_format.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_formatter.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_formatter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_tokenizer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_tokenizer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_tokenizer_factory.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_tokenizer_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_util.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/pinyin_util.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/smart_forest.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/smart_forest.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/smart_get_word.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/pinyin/smart_get_word.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_impl.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/standard/standard_tokenizer_impl.h` | `@@ -27 +27 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/analysis/tokenizer/tokenizer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/analysis/tokenizer/tokenizer_factory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/clucene/clucene_index_reader.cpp` | `@@ -0,0 +1,1209 @@` | Adapted: uses branch-4.2 PrimitiveTypeConvertor for BKD sentinels |
| `be/src/storage/index/inverted/clucene/clucene_index_reader.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/clucene/inverted_index_compaction.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/clucene/inverted_index_compaction.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/clucene/inverted_index_searcher.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/clucene/inverted_index_searcher.h` | `@@ -27 +27 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/clucene/inverted_index_searcher.h` | `@@ -45 +45 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/clucene/inverted_index_writer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/clucene/inverted_index_writer.cpp` | `@@ -20,3 +20,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/clucene/inverted_index_writer.h` | `@@ -26 +25,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/clucene/inverted_index_writer.h` | `@@ -28,2 +27,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/clucene/inverted_index_writer.h` | `@@ -38,0 +38,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/clucene/searcher_cache_value.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/clucene/spi/clucene_index_source.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/clucene/spi/clucene_index_source.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/clucene/spi/clucene_postings_cursor.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/clucene/term_iterator.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/common/inverted_index_cache.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/common/inverted_index_cache.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/common/inverted_index_desc.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/common/inverted_index_profile.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/common/query_info.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/common/single_flight.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/common/snii_query_stats.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/inverted_index_iterator.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/inverted_index_iterator.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/inverted_index_reader.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -20,2 +20 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -23,0 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -34,5 +34,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -42,10 +41,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -53,10 +45,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -70,0 +54,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -77 +62,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -78,0 +64,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -245,0 +234,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -247,0 +244,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -249,2 +249,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -252,0 +257,21 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -269,3 +294,24 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -299,2 +344,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -302,3 +345,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -309,6 +349,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -318,0 +354,10 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/inverted_index_reader.h` | `@@ -321,122 +365,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/conjunction_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/conjunction_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/disjunction_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/engine/all_query/all_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/bit_set_query/bit_set_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/bit_set_query/bit_set_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/bit_set_query/bit_set_weight.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/boolean_query/boolean_query_builder.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/boolean_query/listed_terms.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/boolean_query/listed_terms.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/boolean_query/occur_boolean_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/boolean_query/occur_boolean_weight.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/boolean_query/occur_boolean_weight.h` | `@@ -20 +20,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/boolean_query/occur_boolean_weight.h` | `@@ -22,6 +23,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/boolean_query/occur_boolean_weight.h` | `@@ -61,0 +63,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/boolean_query/occur_boolean_weight.h` | `@@ -66 +69,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/boolean_query/occur_boolean_weight.h` | `@@ -100,3 +104 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/boolean_query/operator_boolean_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/boolean_query/operator_boolean_weight.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/buffered_union_scorer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/buffered_union_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/collect/doc_set_collector.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/collect/doc_set_collector.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/collect/top_k_collector.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/collect/top_k_collector.h` | `@@ -29,2 +29,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/collect/top_k_collector.h` | `@@ -100,0 +101,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/collect/top_k_collector.h` | `@@ -105 +108,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/complete_null_bitmap.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/complete_null_bitmap.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/const_score_query/const_score_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/disjunction_scorer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/disjunction_scorer.h` | `@@ -23,2 +23,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/empty_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/exclude_scorer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/exclude_scorer.h` | `@@ -22 +22 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/expand_query/expand_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/expand_query/expand_weight.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/expand_query/expand_weight.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/intersection.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/intersection.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/intersection_scorer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/intersection_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/match_all_docs_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/null_bitmap_fetcher.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/null_bitmap_fetcher.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/null_masked_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/nullable_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/phrase_prefix_query/phrase_prefix_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/phrase_prefix_query/phrase_prefix_weight.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/phrase_query/multi_phrase_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/phrase_query/multi_phrase_weight.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/phrase_query/phrase_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/phrase_query/phrase_scorer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/phrase_query/phrase_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/phrase_query/phrase_weight.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/phrase_query/phrase_weight.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/postings/listed_walk.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/postings/loaded_postings.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/postings/loaded_postings.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/reqopt_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/score_combiner.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/scored_rows_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/scorer.h` | `@@ -25 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/scorer.h` | `@@ -31,4 +30,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/scorer.h` | `@@ -42,2 +38,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/scorer.h` | `@@ -52,0 +51,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/segment_postings.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/term_query/term_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/term_query/term_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/term_query/term_weight.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/union/buffered_union.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/union/buffered_union.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/union/simple_union.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/union/simple_union.h` | `@@ -23 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/union_postings.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/engine/wand/block_wand.h` | `@@ -24 +24 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/engine/weight.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/block_doc_set.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/chained_conjunction.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/chained_conjunction.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/collect_postings.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/cursor_chained_postings.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/cursor_chained_postings.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/docid_buffer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/docid_set_ops.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/docid_set_ops.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/docid_sink.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/index_source.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/roaring_docid_sink.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/term_pattern.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/term_pattern.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/term_waves.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/exec/truth_set.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/logical/gram/gram_query.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/logical/gram/regex_ast.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/logical/gram/regex_gram_compiler.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/logical/gram/regex_gram_compiler.cpp` | `@@ -29 +29 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/logical/gram/regex_gram_compiler.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/logical/lowering_common.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/logical/lowering_common.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/logical/match_lowering.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/logical/match_lowering.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/logical/node.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/phrase/exact_phrase_matcher.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/phrase/phrase_verifier.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/phrase/position_math.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/phrase/position_span.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/phrase/position_stream.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/phrase/sloppy_phrase_matcher.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/phrase/sloppy_phrase_matcher.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/phrase_edge_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_edge_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_prefix_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_query/exact_phrase_matcher.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_query/ordered_sloppy_phrase_matcher.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_query/ordered_sloppy_phrase_matcher.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_query/phrase_matcher.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_query/phrase_positions.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_query/sloppy_phrase_matcher.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/phrase_query/sloppy_phrase_matcher.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/prefix_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/prefix_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/query_factory.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/query_helper.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/query_helper.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/regexp_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/regexp_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.cpp` | `@@ -21,0 +22,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.cpp` | `@@ -36 +38 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.cpp` | `@@ -38 +40,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.cpp` | `@@ -45 +48,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.cpp` | `@@ -46,0 +56,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.cpp` | `@@ -86,4 +101,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.cpp` | `@@ -147 +160 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.h` | `@@ -20 +20,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.h` | `@@ -44,0 +48,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/bm25_similarity.h` | `@@ -68,0 +77 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/collection_similarity.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/collection_similarity.h` | `@@ -52,13 +51,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/similarity.h` | `@@ -21,0 +22 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/similarity.h` | `@@ -23,0 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/similarity.h` | `@@ -27 +29 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/similarity.h` | `@@ -30 +32 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/scoring/similarity.h` | `@@ -36,3 +37,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/query/spi/index_source.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/spi/io_metrics.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/spi/io_read_batch.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/spi/io_read_batch.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/spi/io_reader.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/spi/position_cursor.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/spi/postings_cursor.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/spi/scoring_context.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/query/term_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/boolean_query/operator_boolean_weight.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/collect/multi_segment_util.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/composite_reader.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/phrase_prefix_query/phrase_prefix_weight.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/phrase_query/multi_phrase_weight.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/phrase_query/phrase_scorer.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/phrase_query/phrase_scorer.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/phrase_query/phrase_weight.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/phrase_query/postings_with_offset.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/prefix_query/prefix_weight.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/regexp_query/regexp_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/regexp_query/regexp_weight.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/regexp_query/regexp_weight.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/scored_bit_set_query/scored_bit_set_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/scored_bit_set_query/scored_bit_set_scorer.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/scored_bit_set_query/scored_bit_set_weight.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/segment_postings.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/query_v2/wildcard_query/wildcard_weight.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -31,6 +31,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -84,0 +85 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -130 +131,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -139 +142 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -146,2 +149,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -165,3 +171,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -192,0 +202 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -203,0 +214 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -248 +259,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -250,3 +264,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -254 +266,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -256,2 +267,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -260,8 +270,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -270 +275 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -274 +279 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -287 +292,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -291 +300 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -295 +303,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -306,2 +314,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -312 +320,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -317 +326,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -323,2 +337 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -331 +344 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -333 +346 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -338,2 +351,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -341 +354 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -403 +416 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -410 +423 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -421 +434 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.cpp` | `@@ -462,2 +475,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.h` | `@@ -33,2 +33,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.h` | `@@ -57 +57,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/collection_statistics.h` | `@@ -103,0 +105 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/inverted_index_selector.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/reader/inverted_index_selector.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -28 +27,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -32,3 +31,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -80 +79 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -139,13 +137,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -156 +142 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -196,7 +181,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -287,30 +266 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -318,11 +268 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -410 +350 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -425 +365 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.cpp` | `@@ -441,170 +380,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.h` | `@@ -23,0 +24 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.h` | `@@ -29 +29,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.h` | `@@ -31 +31,3 @@` | Adapted: FieldType remains in storage/olap_common.h on branch-4.2 |
| `be/src/storage/index/inverted/reader/predicate_collector.h` | `@@ -43,0 +46 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.h` | `@@ -64,0 +68,30 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/predicate_collector.h` | `@@ -83,20 +115,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/reader/text_index_reader.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/reader/text_index_reader.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/bkd/bkd_builder.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/bkd_builder.cpp` | `@@ -31,7 +31,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/bkd_builder.h` | `@@ -27,6 +27,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/bkd_index_block.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/bkd_index_block.cpp` | `@@ -24,3 +24,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/bkd_index_block.h` | `@@ -27,3 +27,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/bkd_reader.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/bkd_reader.h` | `@@ -26,5 +26,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/bkd_types.h` | `@@ -22,3 +22,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/leaf_codec.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/leaf_codec.cpp` | `@@ -27,4 +27,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/leaf_codec.h` | `@@ -24,3 +24,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/point_merger.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/bkd/point_merger.h` | `@@ -26,3 +26,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/point_run.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/bkd/point_run.h` | `@@ -25 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/point_sorter.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/bkd/point_source.h` | `@@ -23 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/bkd/staged_blob_file.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/bkd/staged_blob_file.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/eligibility.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/eligibility.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/posting_cursor.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/posting_cursor.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/posting_run_merger.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/posting_run_merger.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/region_reader.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/region_reader.h` | `@@ -26,3 +26,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/compaction/rowid_conversion.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/snii_index_compaction.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/snii_index_compaction.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/term_cursor.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/term_cursor.h` | `@@ -26,3 +26,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/compaction/term_merge_frontier.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/compaction/term_merge_frontier.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/byte_sink.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/byte_sink.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/byte_source.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/byte_source.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/crc32c.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/encoding/crc32c.h` | `@@ -25 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/encoding/pfor.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/pfor.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/section_framer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/section_framer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/varint.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/zstd_codec.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/encoding/zstd_codec.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/bootstrap_header.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/bootstrap_header.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/bsbf.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/bsbf.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/core_metadata.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/format/core_metadata.cpp` | `@@ -26,2 +26,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/format/core_metadata.h` | `@@ -26,5 +26,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/format/dict_block.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/dict_block.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/dict_block_directory.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/dict_block_directory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/dict_entry.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/dict_entry.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/frq_pod.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/frq_pod.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/frq_prelude.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/frq_prelude.h` | `@@ -24,2 +24,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/format/frq_prelude.h` | `@@ -151,0 +152,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/format/frq_prelude.h` | `@@ -202,7 +205,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/format/metadata_blob.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/metadata_blob.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/metadata_directory.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/metadata_directory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/norms_pod.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/norms_pod.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/null_bitmap.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/null_bitmap.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/prx_decode_stats.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/prx_frame.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/prx_frame.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/prx_pod.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/prx_pod.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/prx_position_iterator.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/prx_position_iterator.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/sampled_term_index.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/sampled_term_index.h` | `@@ -27,3 +27,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/format/tail_pointer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/format/tail_pointer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/io/batch_range_fetcher.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/io/file_reader.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/io/file_writer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/io/io_metrics.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/query/bm25_scorer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/query/bm25_scorer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/query/count_query.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/query/count_query.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/query/docid_posting_reader.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -22,0 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -29,6 +30,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -43,28 +41,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -385,20 +356 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -459,2 +411,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -464,2 +417,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -475,20 +428,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -496 +433,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -498,4 +435,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.cpp` | `@@ -507,20 +442,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.h` | `@@ -23 +22,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.h` | `@@ -27,2 +26,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.h` | `@@ -113,8 +113,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.h` | `@@ -123 +117,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/gram_boolean_query.h` | `@@ -136 +131 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/query/internal/docid_posting_reader.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/query/internal/plain_term_routing.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/query/internal/query_test_counters.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/dict_block_cache.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -29,12 +29,13 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -275,0 +277,16 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -709,0 +727,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -765,11 +787,29 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -776,0 +817 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -781,28 +822,9 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -810,4 +832,10 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -815 +843,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -817,12 +849,31 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -830 +881,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -831,0 +884,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -835,8 +890,18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -844,6 +909,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -850,0 +912,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -852,11 +917,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.cpp` | `@@ -865,2 +924,17 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/reader/logical_index_reader.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/snii_index_source.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/snii_index_source.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/snii_postings_cursor.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/snii_postings_cursor.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/snii_segment_reader.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/snii_segment_reader.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/windowed_posting.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/reader/windowed_posting.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/searcher_cache_value.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_bkd_index_reader.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_bkd_index_reader.h` | `@@ -22,0 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_bkd_index_reader.h` | `@@ -25 +26 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_bkd_index_reader.h` | `@@ -59,2 +60 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_bkd_index_writer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_bkd_index_writer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_bkd_query.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_bkd_query.h` | `@@ -21,2 +21,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_bkd_searcher.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_blob_directory.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_blob_directory.h` | `@@ -30,3 +30,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_blob_staging_directory.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_blob_staging_directory.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -161 +161 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -166 +166 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -209 +208,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -233,3 +231,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -240,4 +236 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -246,2 +239 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -254,4 +246 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -266 +255 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -309 +298 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -343,4 +332 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.cpp` | `@@ -349,3 +335 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_doris_adapter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_index_reader.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_index_reader.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/snii_index_writer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_index_writer.cpp` | `@@ -31,8 +31,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_index_writer.h` | `@@ -30,9 +30,9 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/snii_prx_profile.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/bounded_run_merge.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/compact_posting_pool.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/encoded_spill_run.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/encoded_spill_run.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/global_memory_limiter.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/logical_index_writer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/logical_index_writer.h` | `@@ -29,15 +29,15 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/posting_byte_buffer.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/posting_byte_buffer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/posting_external_sort.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/posting_external_sort.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/posting_prx_encoder.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/posting_prx_encoder.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/posting_window_emitter.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/posting_window_emitter.cpp` | `@@ -28,12 +28,12 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/posting_window_emitter.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/snii_build_memory_tracker.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/snii_build_memory_tracker.h` | `@@ -20 +20 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/snii_compound_writer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/snii_compound_writer.cpp` | `@@ -27,8 +27,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/snii_compound_writer.h` | `@@ -27,4 +27,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/spill_run_codec.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/spill_run_codec.h` | `@@ -28 +28 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/spillable_byte_buffer.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/spimi_term_buffer.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/spimi_term_buffer.cpp` | `@@ -38,8 +38,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/spimi_term_buffer.h` | `@@ -32,3 +32,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/index/inverted/snii/writer/temp_dir.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/snii/writer/term_posting_source.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/index/inverted/util/docid_set_iterator.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/util/fixed_bit_set.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/util/linked_hash_map.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/util/mock_iterator.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/util/priority_queue.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/util/roaring_docid_iterator.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/util/term_position_iterator.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/inverted/util/union_term_iterator.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/io/batch_range_fetcher.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/bm25_scorer.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/bm25_scorer.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/boolean_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/boolean_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/count_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/docid_conjunction.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/docid_set_ops.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/docid_union.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/docid_conjunction.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/docid_set_ops.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/docid_union.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/exact_phrase_stream_matcher.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/phrase_query_split.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/query_test_counters.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/regex_prefix.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/resolved_phrase_plan.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/term_expansion.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/internal/wildcard_matcher.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/phrase_emit.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/phrase_plan.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/phrase_planned_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/phrase_position_source.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/phrase_prefix_exec.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/phrase_prx_validation.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/phrase_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/phrase_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/prefix_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/prefix_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/query_profile.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/query_profile.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/regexp_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/regexp_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/scoring_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/scoring_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/term_expansion.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/term_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/wildcard_query.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/query/wildcard_query.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/snii_index_reader.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/snii_index_reader.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/index/snii/snii_prx_profile.h` | `all hunks` | Ported (verbatim deletion) |
| `be/src/storage/olap_common.h` | `@@ -44,2 +44,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/olap_meta.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/storage/predicate/accept_null_predicate.h` | `all hunks` | Ported (verbatim) |
| `be/src/storage/predicate/comparison_predicate.h` | `@@ -29 +29 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/predicate/in_list_predicate.h` | `@@ -39 +39 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/predicate/null_predicate.cpp` | `@@ -27 +27 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/rowset/beta_rowset.cpp` | `@@ -44,2 +44,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/rowset/beta_rowset_writer.cpp` | `@@ -51,2 +51,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/rowset/beta_rowset_writer_v2.cpp` | `@@ -40,2 +40,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/rowset/rowset.cpp` | `@@ -26 +26 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/rowset/segcompaction.cpp` | `@@ -45,2 +45,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/schema_change/schema_change.cpp` | `@@ -62,2 +62,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/column_reader.cpp` | `@@ -60 +60,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/column_reader.cpp` | `@@ -62,2 +63,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/column_reader.cpp` | `@@ -991 +992,3 @@` | Adapted: passes branch-4.2 ColumnReader row count to CLucene readers |
| `be/src/storage/segment/column_reader.cpp` | `@@ -998,2 +1001,3 @@` | Adapted: passes branch-4.2 ColumnReader row count to CLucene readers |
| `be/src/storage/segment/column_writer.cpp` | `@@ -34 +34 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/column_writer.h` | `@@ -38 +38 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/column_writer.h` | `@@ -512 +512,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/count_on_index_fastpath.h` | `@@ -149 +149 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/segment_iterator.cpp` | `@@ -85 +84,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/segment_iterator.cpp` | `@@ -91,0 +91 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/vertical_segment_writer.cpp` | `@@ -52,0 +53 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/segment/vertical_segment_writer.cpp` | `@@ -54 +54,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/tablet/tablet_schema.cpp` | `@@ -50,2 +50,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/tablet/tablet_schema.h` | `@@ -46 +46 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/task/index_builder.cpp` | `@@ -26,0 +27 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/task/index_builder.cpp` | `@@ -28,2 +29 @@` | Ported (hunk post-image; branch file differs) |
| `be/src/storage/task/index_builder.h` | `all hunks` | Ported (verbatim) |
| `be/src/tools/index_tool.cpp` | `all hunks` | Ported (verbatim) |
| `be/src/tools/kuromoji_build_dict.cpp` | `@@ -37,2 +37,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/exec/scan/scanner_context_test.cpp` | `@@ -290 +289,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exec/scan/scanner_context_test.cpp` | `@@ -295 +293,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exec/scan/scanner_context_test.cpp` | `@@ -302 +299,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exec/scan/scanner_context_test.cpp` | `@@ -310 +306,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exec/scan/scanner_context_test.cpp` | `@@ -316 +311,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exec/scan/scanner_context_test.cpp` | `@@ -322 +316,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exec/scan/scanner_context_test.cpp` | `@@ -328 +321,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/function_is_null_test.cpp` | `@@ -23,0 +24 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/function_match_test.cpp` | `@@ -37,2 +37,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/like_gram_binding_test.cpp` | `@@ -47 +47 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/like_gram_binding_test.cpp` | `@@ -49,2 +49,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_nested_test.cpp` | `@@ -31,2 +31,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_nested_test.cpp` | `@@ -35,3 +35,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_null_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -26,0 +27 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -32,0 +34 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -41,0 +44,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -43,0 +48 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -46 +51,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -48,6 +55,11 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -59,0 +72,34 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -167,0 +214,15 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -212,0 +274,9 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -266,0 +337,16 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -271 +357 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -278,0 +365,49 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -279,0 +415,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -308,2 +445 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -332,0 +469 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -335,0 +473 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -339,0 +478,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -344,0 +487,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -411,5 +555,10 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -507,117 +655,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -696,175 +727,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -947,274 +803,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -1301,96 +883,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -1443,78 +929,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -1555 +964 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -1676,2 +1085,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2053,5 +1463,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2066,2 +1476,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2118,4 +1528,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2129 +1540,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2173,2 +1585,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2366 +1778 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2374 +1786,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2407,4 +1820,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2413,0 +1826,15 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2419,3 +1846 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2432 +1857,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2454,2 +1882,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2460,6 +1888,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2468,2 +1892,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2476,3 +1900,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2482,0 +1907,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2486,0 +1917,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2497,2 +1929,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2526,0 +1959 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2539 +1972 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2551 +1984 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2554,5 +1987,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2567 +1999 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2582 +2014 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2585,2 +2017,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2590,3 +2022,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2597,3 +2030,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2604,22 +2037,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2627,4 +2040 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2634 +2044 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2636 +2046 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2645 +2055 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2652,2 +2062,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2655,7 +2065,11 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2664,5 +2078,25 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2670,3 +2104,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2675,0 +2111,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2686 +2123 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2690 +2126,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2693,2 +2129,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2696,4 +2133,10 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2702,4 +2145,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2706,0 +2151 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2708 +2153 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2711,0 +2157,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2729,2 +2176,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2732,3 +2179 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2736 +2181,10 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2740,4 +2194,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2751 +2203 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2769,2 +2221,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2774,3 +2226,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2780,3 +2231,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2787,6 +2238,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2817,2 +2264,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2826,3 +2273,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2833,10 +2280,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2850 +2289,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2868,2 +2308,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2873,5 +2313 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2882,3 +2318,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2886,0 +2323,39 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2889 +2364,43 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2923 +2440,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2941,2 +2459,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2946,3 +2464 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2952,3 +2468,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2956,0 +2473 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2960,0 +2478,130 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -2962 +2609 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3016,0 +2664,11 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3032,5 +2690,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3043 +2700,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3046,2 +2704,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3051,3 +2713,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3055,0 +2718 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3068,2 +2731,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3073,34 +2735,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3114 +2743 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3118,3 +2747,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3134,6 +2763,50 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3143,3 +2816,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3147 +2821,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3151,2 +2832 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3154,3 +2834,218 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3159 +3054,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3194,2 +3090,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3204,2 +3100,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3227,0 +3124,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3316 +3219,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3337,2 +3241,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3387 +3291,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3398,2 +3303,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3447 +3352,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3458,2 +3364,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3471 +3377 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3506,14 +3411,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3773,69 +3664,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -3959 +3782 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/function_search_test.cpp` | `@@ -4001,0 +3825,219 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/function/search/search_lowering_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/exprs/vsearch_expr_test.cpp` | `@@ -37,0 +38,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/vsearch_expr_test.cpp` | `@@ -39 +40,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/vsearch_expr_test.cpp` | `@@ -1577,19 +1577,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/exprs/vsearch_expr_test.cpp` | `@@ -1598,4 +1579,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/runtime/index_policy/index_policy_mgr_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/cloud_file_cache_write_index_only_test.cpp` | `@@ -41 +41 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/ann/ann_index_writer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/ann/vector_search_utils.h` | `@@ -41,0 +42,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/ann/vector_search_utils.h` | `@@ -44,2 +45,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/index_builder_test.cpp` | `@@ -32 +32 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/index_common_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/index_compound_reader_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/index_fs_directory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/analysis_factory_mgr_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/analyzer/analyzer_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/analyzer/analyzer_test.cpp` | `@@ -22,0 +23,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/analyzer/analyzer_test.cpp` | `@@ -25,2 +32,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/analyzer/analyzer_test.cpp` | `@@ -347,0 +356,146 @@` | Adapted: retained available-parser coverage; Kuromoji is absent |
| `be/test/storage/index/inverted/analysis/analyzer/custom_analyzer_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/analyzer/custom_analyzer_test.cpp` | `@@ -33,6 +33,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/analyzer/custom_analyzer_test.cpp` | `@@ -341,170 +338,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/analyzer/icu_analyzer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/analyzer/ik_anayzer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/darts_smoke_test.cpp` | `@@ -25 +25 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/darts_smoke_test.cpp` | `@@ -31 +31 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/darts_smoke_test.cpp` | `@@ -48 +48 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/darts_smoke_test.cpp` | `@@ -52 +52 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dict_format_test.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_builder_test.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_builder_test.cpp` | `@@ -25 +25 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_builder_test.cpp` | `@@ -43,2 +43,3 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_builder_test.cpp` | `@@ -56 +57 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_test.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_test.cpp` | `@@ -31,2 +31,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_test.cpp` | `@@ -44,2 +44,3 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_test.cpp` | `@@ -57 +58 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_test.cpp` | `@@ -74 +75 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_dictionary_test.cpp` | `@@ -80 +81 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_ipadic_parser_test.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_ipadic_parser_test.cpp` | `@@ -24 +24 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_ipadic_parser_test.cpp` | `@@ -43 +43 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_normalize_test.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_normalize_test.cpp` | `@@ -27 +27,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_normalize_test.cpp` | `@@ -29 +30 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_normalize_test.cpp` | `@@ -42 +43 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_normalize_test.cpp` | `@@ -44 +45 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -31,4 +31,4 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -76 +76 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -94 +94 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -97 +97 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -111 +111 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -114 +114 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -161 +161 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -168,3 +168,4 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -172,2 +173,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -175 +176 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_real_dict_test.cpp` | `@@ -177 +178 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -18 +18 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -28,4 +28,4 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -36 +36 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -48,2 +48,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -65,3 +65,3 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -95 +95 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -100 +100 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -123 +123 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -133 +133 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -150 +150 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -166 +166 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -186,5 +186,5 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -218 +218 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -224 +224 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji/kuromoji_viterbi_test.cpp` | `@@ -230 +230 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji_analyzer_test.cpp` | `@@ -26,2 +26,2 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/kuromoji_tokenizer_test.cpp` | `@@ -24 +24 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/analyzer/simple_analyzer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/char_filter/char_filter_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/char_filter/char_replace_char_filter_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/char_filter/char_replace_char_filter_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/char_filter/icu_normalizer_char_filter_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/gram/gram_density_corpus_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/gram/gram_density_test.cpp` | `@@ -35 +35 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/gram/gram_density_test.cpp` | `@@ -43,2 +43,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/gram/gram_df_distribution_test.cpp` | `@@ -46 +46 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/gram/gram_extractor_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/gram/gram_scheme_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/inverted_index_parser_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/kuromoji_parser_type_test.cpp` | `@@ -23 +23 @@` | N-A: branch-4.2 has no Kuromoji parser or dictionary sources |
| `be/test/storage/index/inverted/analysis/reader_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/setting_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/token_filter/ascii_folding_filter_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/token_filter/icu_normalizer_filter_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/token_filter/lower_case_filter_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/token_filter/pinyin_filter_test.cpp` | `@@ -27,15 +27,15 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/token_filter/pinyin_filter_test.cpp` | `@@ -1893 +1893 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/token_filter/pinyin_filter_test.cpp` | `@@ -1895 +1895 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/token_filter/word_delimiter_filter_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/token_filter/word_delimiter_filter_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/token_filter/word_delimiter_iterator_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/basic_tokenizer_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/char_group_tokenizer_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/edge_ngram_tokenizer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/gram_tokenizer_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/tokenizer/gram_tokenizer_test.cpp` | `@@ -26,2 +26,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/analysis/tokenizer/icu_tokenizer_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/keyword_analyzer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/ngram_tokenizer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/pinyin_alphabet_tokenizer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/pinyin_analysis_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/pinyin_util_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/smart_forest_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/smart_get_word_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/analysis/tokenizer/standard_tokenizer_factory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/clucene/compaction/index_compaction_performance_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/clucene/compaction/index_compaction_test.cpp` | `@@ -23 +23 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/clucene/compaction/util/index_compaction_utils.h` | `@@ -37 +37,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/clucene/compaction/util/index_compaction_utils.h` | `@@ -211,7 +215,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/clucene/compaction/util/index_compaction_utils.h` | `@@ -219 +218,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/clucene/compaction/util/index_compaction_utils.h` | `@@ -249,7 +250,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/clucene/compaction/util/index_compaction_utils.h` | `@@ -257 +253,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/clucene/compaction/util/index_compaction_utils.h` | `@@ -269 +267 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/clucene/compaction/util/index_compaction_utils.h` | `@@ -281 +279 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/clucene/compaction/util/index_compaction_utils.h` | `@@ -408 +406 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/clucene/compaction/util/index_compaction_utils.h` | `@@ -414 +412,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/common/inverted_index_profile_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/common/single_flight_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/common/string_helper_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/empty_index_file_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -32 +31,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -34,4 +33,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -39 +39,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -199 +201 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -280,2 +281,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -286 +286 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -320,0 +321,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -324,2 +328,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -328,3 +331,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -335 +337 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -338,7 +340 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -347 +343 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -350,2 +346,36 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -356 +386 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -358 +388 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -362 +392 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -364 +394 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/inverted_index_reader_analysis_purpose_test.cpp` | `@@ -383 +413 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/conjunction_query_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/disjunction_query_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/engine/boolean_kernel_bench_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/boolean_postings_bench_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/boolean_query/boolean_query_builder_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/boolean_query/listed_terms_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -29,6 +29,10 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -158,2 +162 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -168,2 +171 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -179,3 +181,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -305,3 +307 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -313,3 +313,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -346,3 +347 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -351,2 +350 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -361,3 +359,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -387 +385 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -412,2 +410 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -419,2 +416 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -429,3 +425,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -477 +473,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -481,4 +478,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -492,6 +489,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -513,6 +506,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -557,2 +546,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -568,2 +557 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -608,2 +596,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_query_test.cpp` | `@@ -619,2 +607 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/boolean_tree_contract_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/boolean_truth_contract_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/buffered_union_scorer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/buffered_union_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/disjunction_scorer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/doc_set_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/exclude_scorer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/intersection_scorer_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/intersection_scorer_test.cpp` | `@@ -29 +29 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/intersection_scorer_test.cpp` | `@@ -46,2 +46,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/intersection_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/loaded_postings_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -21,0 +22 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -23,0 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -24,0 +27 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -29,4 +32,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -262,2 +268,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -310,2 +316,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -358,2 +364,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -411,2 +417,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -459,2 +465,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -512,2 +518,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -566 +572 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -569,2 +575,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -621,2 +627,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -713,2 +719,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -773,2 +779,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_phrase_query_test.cpp` | `@@ -794,0 +801,35 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/multi_segment_collector_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/null_bitmap_test_util.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/occur_boolean_kernel_bench_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/occur_boolean_query_real_index_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/occur_boolean_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/phrase_kernel_bench_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/phrase_postings_bench_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/phrase_prefix_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/phrase_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/phrase_scorer_contract_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/prefix_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -18,2 +17,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -30,3 +28,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -52 +53,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -59,8 +66,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -68 +72 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -103,0 +108,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -124 +133,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -132 +142 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -144 +154,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -160 +171,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -167 +179 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -184 +196,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -189,2 +202,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -207,0 +221,28 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -230 +271,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -235,2 +277,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -269 +311,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -274,2 +317,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -306 +349,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -311 +355 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -314,2 +358,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -342 +386,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -365 +410,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -370,2 +416,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -398 +444,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -403 +450,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -408 +456 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -420 +468,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -425,2 +474,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -443 +492 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -455 +504,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -460,2 +510,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -478 +528 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -490 +540,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -495,2 +546,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -513 +564 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -525 +576,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -530,2 +582,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -548 +600 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -560 +612,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | `@@ -565,2 +618,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/regexp_wildcard_lowercase_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/reqopt_scorer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/score_combiner_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/segment_postings_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/simple_union_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/top_k_collector_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/union_postings_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -18,2 +17,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -30,3 +28,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -127 +128,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -135 +137 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -148 +150,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -155 +158 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -172 +175,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -177,2 +181,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -209 +213,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -214,2 +219,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -245 +250,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -250,2 +256,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -281 +287,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -286,2 +293,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -317 +324,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -322,2 +330,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -353 +361,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -358,2 +367,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -389 +398,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -394 +404 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -397,2 +407,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -425 +435,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -448 +459,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -453,2 +465,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -484 +496,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -489,2 +502,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -518 +531,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/engine/wildcard_query_test.cpp` | `@@ -523 +537,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/exec/block_doc_set_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/exec/chained_conjunction_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/exec/cursor_chained_postings_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/exec/docid_kernel_bench_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/exec/docid_sink_contract_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/logical/gram/gram_literal_coverage_test.cpp` | `@@ -41 +41 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/logical/gram/gram_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/logical/gram/regex_ast_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/logical/gram/regex_gram_compiler_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/logical/gram/regex_gram_fuzz_test.cpp` | `@@ -25,2 +25,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/logical/gram/regex_gram_no_false_negative_test.cpp` | `@@ -43,2 +43,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/query/logical/gram/regex_gram_recall_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/phrase/exact_phrase_stream_matcher_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/phrase/position_math_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/phrase/sloppy_phrase_matcher_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/phrase_edge_query_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/phrase_prefix_query_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/phrase_query/exact_phrase_matcher_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/phrase_query/ordered_sloppy_phrase_matcher_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/phrase_query/phrase_freq_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/phrase_query/phrase_matcher_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/phrase_query/phrase_positions_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/phrase_query/phrase_queue_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/phrase_query/sloppy_phrase_matcher_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/phrase_query_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/query_helper_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/regexp_query_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query/scoring/bm25_similarity_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/scoring/collection_similarity_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/spi/fake_index_source.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/spi/io_read_batch_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query/spi/scoring_context_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query_info_test_util.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/query_v2/composite_reader_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query_v2/multi_segment_collector_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query_v2/phrase_query_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query_v2/prefix_query_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/query_v2/segment_postings_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -35,0 +36 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -48,10 +49,10 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -317 +318,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -1473 +1475,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -1542 +1545,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -1627,2 +1631,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -1704,2 +1708,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -1835 +1839,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -2048 +2053,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -2501,2 +2507,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/reader/collection_statistics_test.cpp` | `@@ -2560,2 +2566,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bench/README.md` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/bench/bkd_native_vs_clucene_bench_test.cpp` | `@@ -75,8 +75,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bench/phrase_candidate_pushdown_bench_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/bench/search_tree_bench_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/bench/snii_vs_v3_benchmark_test.cpp` | `@@ -113,3 +113,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bench/snii_vs_v3_benchmark_test.cpp` | `@@ -117 +118,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bench/snii_vs_v3_benchmark_test.cpp` | `@@ -339 +339,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bench/snii_vs_v3_benchmark_test.cpp` | `@@ -341,8 +340,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bench/snii_vs_v3_benchmark_test.cpp` | `@@ -1846,2 +1838 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bench/snii_vs_v3_benchmark_test.cpp` | `@@ -1858,4 +1849,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bench/snii_vs_v3_benchmark_test.cpp` | `@@ -1867,5 +1856,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bench/snii_vs_v3_benchmark_test.cpp` | `@@ -1873,3 +1860 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_builder_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_builder_test.cpp` | `@@ -30,9 +30,9 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_container_roundtrip_test.cpp` | `@@ -43,10 +43,10 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_corruption_test.cpp` | `@@ -58,11 +58,11 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_differential_test.cpp` | `@@ -62,8 +62,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_format_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_format_test.cpp` | `@@ -25,2 +25,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_golden_format_test.cpp` | `@@ -50,10 +50,10 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_index_block_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_index_block_test.cpp` | `@@ -27,5 +27,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_property_test.cpp` | `@@ -55,7 +55,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_reader_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_reader_test.cpp` | `@@ -32,7 +32,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_types_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/bkd_types_test.cpp` | `@@ -25 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/leaf_codec_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/leaf_codec_test.cpp` | `@@ -28,6 +28,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/point_merger_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/bkd/point_run_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/bkd/point_sorter_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/point_sorter_test.cpp` | `@@ -29 +29 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/snii_bkd_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/bkd/staged_blob_file_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/bkd/staged_blob_file_test.cpp` | `@@ -28 +28 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/common/slice_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/compaction/snii_compaction_eligibility_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/compaction/snii_index_compaction_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/compaction/snii_logical_null_reader_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/compaction/snii_memory_reporter_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/compaction/snii_posting_cursor_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/compaction/snii_rowid_conversion_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/compaction/snii_streamed_session_test.cpp` | `@@ -35,20 +35,19 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/compaction/snii_term_cursor_test.cpp` | `@@ -47,13 +47,13 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/encoding/byte_sink_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/encoding/byte_source_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/encoding/crc32c_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/encoding/pfor_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/encoding/section_framer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/encoding/varint_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/encoding/zstd_codec_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/bootstrap_header_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/bsbf_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/core_metadata_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/format/core_metadata_test.cpp` | `@@ -29,4 +29,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/format/dict_block_directory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/dict_block_owned_finish_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/dict_block_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/dict_entry_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/format_constants_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/frq_pod_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/frq_prelude_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/gram_index_section_sizes_test.cpp` | `@@ -43,4 +43,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/format/gram_stop_gram_curve_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/metadata_blob_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/metadata_directory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/norms_pod_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/null_bitmap_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/prx_pod_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/format/prx_pod_test.cpp` | `@@ -30,8 +30,8 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/format/prx_position_iterator_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/sampled_term_index_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/format/tail_pointer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/io/batch_range_fetcher_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/io/file_reader_caller_buffer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/io/local_file.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/io/local_file.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/io/local_file_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/io/local_file_test.cpp` | `@@ -20,0 +21 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/io/local_file_test.cpp` | `@@ -35 +36,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/io/local_file_test.cpp` | `@@ -80 +82 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/io/metered_file_reader.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/io/metered_file_reader.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/io/metered_file_reader_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/boolean_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/byte_skip_test.cpp` | `@@ -32,30 +32,18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/byte_skip_test.cpp` | `@@ -66,2 +54,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/byte_skip_test.cpp` | `@@ -400 +388 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/byte_skip_test.cpp` | `@@ -408,2 +396,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/byte_skip_test.cpp` | `@@ -415 +403,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/byte_skip_test.cpp` | `@@ -423,2 +412 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/chained_conjunction_io_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/count_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/gram_boolean_query_e2e_test.cpp` | `@@ -57,9 +57,9 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/gram_boolean_query_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/gram_boolean_query_test.cpp` | `@@ -29,7 +29,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/pattern_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/phrase_candidate_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/phrase_prefix_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/phrase_skip_test.cpp` | `@@ -32,13 +32,11 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/phrase_skip_test.cpp` | `@@ -641,3 +638,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/phrase_skip_test.cpp` | `@@ -687,4 +681,0 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/phrase_streaming_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/posting_grouping_test.cpp` | `@@ -31,18 +31,17 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/posting_grouping_test.cpp` | `@@ -70,2 +69,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/posting_grouping_test.cpp` | `@@ -302 +301,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/posting_grouping_test.cpp` | `@@ -403 +403 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/query/prefix_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/query_term_resolution_batch_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/query/top_k_scores.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/reader/snii_end_to_end_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/reader/snii_index_source_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/reader/snii_postings_cursor_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/reader/snii_rewrite_snapshot_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/reader/snii_segment_reader_blob_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/reader/snii_segment_reader_open_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_ann_container_test.cpp` | `@@ -69,0 +70 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/snii_ann_container_test.cpp` | `@@ -72,7 +73,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/snii_b2_or_read_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_b2_t07_dict_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_b2_t08_wildcard_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_b2_t10_cover_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_bkd_adapter_test.cpp` | `@@ -51,12 +51,12 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/snii_bkd_adapter_types_test.cpp` | `@@ -57,6 +57,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/snii_blob_bkd_roundtrip_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_blob_directory_test.cpp` | `@@ -28 +28 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/snii_blob_directory_test.cpp` | `@@ -45,2 +45,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/snii_clucene_guard_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_crc32c_equiv_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_doris_adapter_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_frq_direct_encode_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_frq_prelude_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_golden_corpus_test.cpp` | `@@ -57,5 +57,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/snii_gram_cache_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_index_reader_analyzed_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_index_reader_candidate_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_index_reader_count_fallback_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_query_oracle.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_query_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_query_test_util.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_spill_io_test.cpp` | `@@ -43,3 +43,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/snii_spimi_intern_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_variant_routing_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/snii_writer_test.cpp` | `@@ -43,17 +43,17 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/snii_writer_test.cpp` | `@@ -775 +775 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/spimi_dense_rank_order_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/spimi_spill_rank_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/staged_file_probe.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/stats/snii_stats_provider.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/stats/snii_stats_provider.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/stats/snii_stats_provider_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/bounded_posting_codec_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/compact_posting_pool_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/compact_posting_pool_test.cpp` | `@@ -29 +29 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/gram_density_calibration_test.cpp` | `@@ -38,3 +38,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/high_df_digest_cost_test.cpp` | `@@ -45,6 +45,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/logical_index_writer_freq_stats_test.cpp` | `@@ -30,13 +30,13 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/memory_reporter_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/memory_reporter_wiring_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/phase_a_readback_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/posting_interleave_test.cpp` | `@@ -28,15 +28,14 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/posting_window_emitter_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/snii_build_memory_tracker_test.cpp` | `@@ -37 +37 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_build_memory_tracker_test.cpp` | `@@ -46 +46 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_compound_writer_blob_test.cpp` | `@@ -37,11 +37,11 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_compound_writer_inherit_test.cpp` | `@@ -47,13 +47,13 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_compound_writer_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_compound_writer_test.cpp` | `@@ -37,27 +37,26 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_compound_writer_test.cpp` | `@@ -437 +436 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_compound_writer_test.cpp` | `@@ -477 +476 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_gram_inherit_test.cpp` | `@@ -46,2 +46,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_gram_inherit_test.cpp` | `@@ -49,4 +49,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_no_bigram_writer_test.cpp` | `@@ -38,5 +38,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_prx_tier_writer_test.cpp` | `@@ -45,5 +45,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/snii_writer_golden_bytes_test.cpp` | `@@ -65,5 +65,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/spill_run_codec_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/spill_run_codec_test.cpp` | `@@ -34,4 +34,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/spillable_byte_buffer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/spimi_global_memory_limiter_test.cpp` | `@@ -32,6 +32,6 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/spimi_locality_bench_test.cpp` | `@@ -32,3 +32,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/spimi_resident_accounting_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/spimi_run_cap_test.cpp` | `@@ -27,2 +27,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/spimi_spill_memory_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/spimi_spill_writer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/spimi_streaming_writer_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/spimi_term_buffer_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/spimi_term_buffer_test.cpp` | `@@ -33 +33 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/stop_gram_test.cpp` | `@@ -41,7 +41,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/index/inverted/snii/writer/temp_dir_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/term_posting_source_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/snii/writer/term_posting_test_utils.h` | `all hunks` | Ported (verbatim) |
| `be/test/storage/index/inverted/util/fixed_bit_set_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/util/linked_hash_map_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/util/mock_iterator_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/util/priority_queue_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/inverted/util/union_term_iterator_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/snii/bench/phrase_candidate_pushdown_bench_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/snii/io/batch_range_fetcher_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/snii/query/docid_set_ops_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/snii/query/query_operator_error_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/snii/query/query_profile_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/snii/query/query_term_resolution_batch_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/index/snii/query/scoring_query_test.cpp` | `all hunks` | Ported (verbatim deletion) |
| `be/test/storage/predicate/accept_null_predicate_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/rowid_conversion_test.cpp` | `@@ -54 +54 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/rowset/segment_flusher_format_test.cpp` | `@@ -69,0 +70,2 @@` | N-A: this test file is absent on branch-4.2; the source hunk only updates moved include paths |
| `be/test/storage/rowset/segment_flusher_format_test.cpp` | `@@ -72,4 +74,2 @@` | N-A: this test file is absent on branch-4.2; the source hunk only updates moved include paths |
| `be/test/storage/segment/index_reader_helper_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/segment/inverted_index_array_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/segment/inverted_index_file_reader_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/segment/inverted_index_file_writer_test.cpp` | `@@ -17,0 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_file_writer_test.cpp` | `@@ -23,0 +25 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_file_writer_test.cpp` | `@@ -27 +29,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_file_writer_test.cpp` | `@@ -1049 +1052,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_file_writer_test.cpp` | `@@ -1182 +1186,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_file_writer_test.cpp` | `@@ -1192 +1197,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_file_writer_test.cpp` | `@@ -1283,2 +1289,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_file_writer_test.cpp` | `@@ -1336 +1342,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_iterator_test.cpp` | `@@ -37,3 +37,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_query_type_test.cpp` | `@@ -18 +18 @@` | N-A: this test file is absent on branch-4.2; the source hunk only updates moved include paths |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -24,0 +25,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -25,0 +28 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -29,0 +33 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -31,0 +36,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -33,0 +41 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -34,0 +43 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -37,2 +46,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -40 +51,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -47,0 +61,7 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -171,0 +192,39 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -398 +457 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -2110,2 +2169,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -2133,2 +2192,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -2576,3 +2635,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -2641,3 +2700,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -2789,0 +2848,313 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -3498 +3869 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -3507 +3878 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -4587,0 +4959,20 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_reader_test.cpp` | `@@ -4689,0 +5081,174 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_searcher_cache_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/segment/inverted_index_searcher_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_writer_test.cpp` | `@@ -18 +18 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_writer_test.cpp` | `@@ -47,4 +47,5 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_writer_test.cpp` | `@@ -52,4 +53,4 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_writer_test.cpp` | `@@ -359 +360 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_writer_test.cpp` | `@@ -857 +858 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/inverted_index_writer_test.cpp` | `@@ -1096 +1097 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/segment/search_function_query_cache_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/segment/segment_corruption_test.cpp` | `@@ -35,3 +35,3 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/tablet/tablet_schema_index_test.cpp` | `all hunks` | Ported (verbatim) |
| `be/test/storage/variant/variant_column_writer_reader_test.cpp` | `@@ -50,2 +50,2 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/storage/variant/variant_column_writer_reader_test.cpp` | `@@ -61 +61 @@` | Ported (hunk post-image; branch file differs) |
| `be/test/testutil/benchmark_control.h` | `all hunks` | Ported (verbatim) |
| `be/test/testutil/index_storage_test_util.cpp` | `@@ -51 +51 @@` | Ported (hunk post-image; branch file differs) |
| `build-support/check-header-deps.py` | `@@ -72 +72 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -73,0 +74,65 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -80,2 +145,2 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -399,0 +465,2 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -400,0 +468,10 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -452 +529 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -458,7 +535 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -498,9 +569,2 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -581,0 +646,12 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -628,3 +704,4 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -632,0 +710,7 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -663 +747 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -665,4 +749,6 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -670,0 +757,8 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/check-header-deps.py` | `@@ -908 +1002 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/compile-bench/rebuild_radius.py` | `@@ -50 +50 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/run-query-engine-paired-benchmarks.py` | `all hunks` | Ported (verbatim) |
| `build-support/tests/test-build-hygiene-header-deps.sh` | `@@ -46,0 +47 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `build-support/tests/test-build-hygiene-header-deps.sh` | `@@ -78,0 +80,21 @@` | N-A: this build-support tool does not exist on branch-4.2 |
| `regression-test/data/inverted_index_p0/test_match_format_parity.out` | `@@ -0,0 +1,118 @@` | Ported (hunk post-image; branch file differs) |
| `regression-test/data/search/test_search_not_null_bitmap.out` | `all hunks` | Ported (verbatim) |
| `regression-test/data/search/test_search_segment_document_ids.out` | `@@ -0,0 +1,53 @@` | Ported (hunk post-image; branch file differs) |
| `regression-test/suites/inverted_index_p0/test_match_format_parity.groovy` | `all hunks` | Ported (verbatim) |
| `regression-test/suites/search/test_search_format_parity.groovy` | `all hunks` | Ported (verbatim) |
| `regression-test/suites/search/test_search_inverted_index_profile.groovy` | `all hunks` | Ported (verbatim) |
| `regression-test/suites/search/test_search_not_null_bitmap.groovy` | `all hunks` | Ported (verbatim) |
| `regression-test/suites/search/test_search_segment_document_ids.groovy` | `all hunks` | Ported (verbatim) |
| `run-be-ut.sh` | `@@ -127,0 +128,3 @@` | Ported (hunk post-image; branch file differs) |
| `run-be-ut.sh` | `@@ -564,2 +567,3 @@` | Ported (hunk post-image; branch file differs) |
| `run-be-ut.sh` | `@@ -596,0 +601 @@` | Ported (hunk post-image; branch file differs) |

## Unicode data in the PR diff

The English prose check classifies 1,036 changed literals as Unicode data. Of these, 1,023 are exact literals already present at their old paths on `origin/branch-4.2`; the refactor only moved those files. The 13 new literals are Chinese query/index fixtures in regex and wildcard unit tests (9) and MATCH format-parity regression SQL and DSL (4). The two production literals are existing data: a Chinese numeral lookup string and the Kelvin-sign regex case-folding literal. Each literal receives an individual ID-bound exception in the pre-push check; no comment, message, or whole-file exception is allowed.

| File | Existing literals moved | New fixture literals |
| --- | ---: | ---: |
| `be/src/storage/index/inverted/analysis/analyzer/ik/core/CN_QuantifierSegmenter.cpp` | 1 | 0 |
| `be/src/storage/index/inverted/query/logical/gram/regex_ast.cpp` | 1 | 0 |
| `be/test/exprs/function/search/function_search_test.cpp` | 2 | 0 |
| `be/test/storage/index/inverted/analysis/analyzer/custom_analyzer_test.cpp` | 1 | 0 |
| `be/test/storage/index/inverted/analysis/analyzer/icu_analyzer_test.cpp` | 254 | 0 |
| `be/test/storage/index/inverted/analysis/analyzer/ik_anayzer_test.cpp` | 184 | 0 |
| `be/test/storage/index/inverted/analysis/analyzer/simple_analyzer_test.cpp` | 13 | 0 |
| `be/test/storage/index/inverted/analysis/char_filter/char_replace_char_filter_factory_test.cpp` | 2 | 0 |
| `be/test/storage/index/inverted/analysis/char_filter/char_replace_char_filter_test.cpp` | 1 | 0 |
| `be/test/storage/index/inverted/analysis/gram/gram_extractor_test.cpp` | 3 | 0 |
| `be/test/storage/index/inverted/analysis/token_filter/ascii_folding_filter_factory_test.cpp` | 2 | 0 |
| `be/test/storage/index/inverted/analysis/token_filter/lower_case_filter_factory_test.cpp` | 2 | 0 |
| `be/test/storage/index/inverted/analysis/token_filter/pinyin_filter_test.cpp` | 69 | 0 |
| `be/test/storage/index/inverted/analysis/token_filter/word_delimiter_iterator_test.cpp` | 5 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/basic_tokenizer_factory_test.cpp` | 23 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/char_group_tokenizer_factory_test.cpp` | 12 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/edge_ngram_tokenizer_test.cpp` | 3 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/gram_tokenizer_test.cpp` | 2 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/icu_tokenizer_factory_test.cpp` | 22 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/ngram_tokenizer_test.cpp` | 3 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/pinyin_analysis_test.cpp` | 138 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/pinyin_util_test.cpp` | 92 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/smart_forest_test.cpp` | 41 | 0 |
| `be/test/storage/index/inverted/analysis/tokenizer/smart_get_word_test.cpp` | 89 | 0 |
| `be/test/storage/index/inverted/common/string_helper_test.cpp` | 15 | 0 |
| `be/test/storage/index/inverted/query/engine/regexp_query_test.cpp` | 0 | 5 |
| `be/test/storage/index/inverted/query/logical/gram/regex_ast_test.cpp` | 2 | 0 |
| `be/test/storage/index/inverted/query/logical/gram/regex_gram_fuzz_test.cpp` | 3 | 0 |
| `be/test/storage/index/inverted/query/logical/gram/regex_gram_recall_test.cpp` | 6 | 0 |
| `be/test/storage/index/inverted/snii/query/gram_boolean_query_e2e_test.cpp` | 6 | 0 |
| `be/test/storage/index/inverted/snii/query/pattern_query_test.cpp` | 2 | 0 |
| `be/test/storage/index/inverted/snii/snii_b2_t08_wildcard_test.cpp` | 15 | 2 |
| `be/test/storage/index/inverted/snii/snii_golden_corpus_test.cpp` | 5 | 0 |
| `be/test/storage/index/inverted/snii/snii_query_test.cpp` | 0 | 2 |
| `be/test/storage/index/inverted/snii/snii_spimi_intern_test.cpp` | 1 | 0 |
| `be/test/storage/index/inverted/snii/snii_writer_test.cpp` | 2 | 0 |
| `be/test/storage/index/inverted/snii/writer/snii_writer_golden_bytes_test.cpp` | 1 | 0 |
| `regression-test/suites/inverted_index_p0/test_match_format_parity.groovy` | 0 | 4 |

## Branch-4.2 API adaptations

- The nested SEARCH binding uses `SchemaSPtr` column IDs from the scan schema and resolves the physical VARIANT reader by the bound root UID. This preserves the source behavior when a root is dropped and recreated with the same name.
- The SEARCH Top-K collector receives the segment delete bitmap through `IndexQueryContext`; otherwise deleted rows could consume the Top-K quota.
- CLucene BKD sentinels use the branch's `PrimitiveTypeConvertor` because `StorageLayout` is unavailable.
- Collection statistics use branch-4.2 segment IDs, and CLucene readers receive the branch's column reader row count.
- Files that previously relied on headers moved by this PR now include their required declarations directly, including `CppTypeTraits` for page encoding.
- The test-only temporary file directory setter is defined in `exec_env_init.cpp`, where its complete type is available after the writer header move.
- The branch-4.2 VARIANT test keeps its `RowIdStorageReader` coverage and the header that declares it.
- Kuromoji code and its metadata are omitted because branch-4.2 has no Kuromoji parser or dictionary sources.
