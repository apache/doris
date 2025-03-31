// Copyright (c) Microsoft Corporation. All rights reserved.
// Licensed under the MIT license.

#pragma once

#include <string.h>

namespace program_options_utils
{
const std::string make_program_description(const char *executable_name, const char *description)
{
    return std::string("\n")
        .append(description)
        .append("\n\n")
        .append("Usage: ")
        .append(executable_name)
        .append(" [OPTIONS]");
}

// Required parameters
const char *DATA_TYPE_DESCRIPTION = "data type, one of {int8, uint8, float} - float is single precision (32 bit)";
const char *DISTANCE_FUNCTION_DESCRIPTION =
    "distance function {l2, mips, fast_l2, cosine}.  'fast l2' and 'mips' only support data_type float";
const char *INDEX_PATH_PREFIX_DESCRIPTION = "Path prefix to the index, e.g. '/mnt/data/my_ann_index'";
const char *RESULT_PATH_DESCRIPTION =
    "Path prefix for saving results of the queries, e.g. '/mnt/data/query_file_X.bin'";
const char *QUERY_FILE_DESCRIPTION = "Query file in binary format, e.g. '/mnt/data/query_file_X.bin'";
const char *NUMBER_OF_RESULTS_DESCRIPTION = "Number of neighbors to be returned (K in the DiskANN white paper)";
const char *SEARCH_LIST_DESCRIPTION =
    "Size of search list to use.  This value is the number of neighbor/distance pairs to keep in memory at the same "
    "time while performing a query.  This can also be described as the size of the working set at query time.  This "
    "must be greater than or equal to the number of results/neighbors to return (K in the white paper).  Corresponds "
    "to L in the DiskANN white paper.";
const char *INPUT_DATA_PATH = "Input data file in bin format.  This is the file you want to build the index over.  "
                              "File format:  Shape of the vector followed by the vector of embeddings as binary data.";

// Optional parameters
const char *FILTER_LABEL_DESCRIPTION =
    "Filter to use when running a query.  'filter_label' and 'query_filters_file' are mutually exclusive.";
const char *FILTERS_FILE_DESCRIPTION =
    "Filter file for Queries for Filtered Search.  File format is text with one filter per line.  File must "
    "have exactly one filter OR the same number of filters as there are queries in the 'query_file'.";
const char *LABEL_TYPE_DESCRIPTION =
    "Storage type of Labels {uint/uint32, ushort/uint16}, default value is uint which will consume memory 4 bytes per "
    "filter.  'uint' is an alias for 'uint32' and 'ushort' is an alias for 'uint16'.";
const char *GROUND_TRUTH_FILE_DESCRIPTION =
    "ground truth file for the queryset"; // what's the format, what's the requirements? does it need to include an
                                          // entry for every item or just a small subset? I have so many questions about
                                          // this file
const char *NUMBER_THREADS_DESCRIPTION = "Number of threads used for building index.  Defaults to number of logical "
                                         "processor cores on your this machine returned by omp_get_num_procs()";
const char *FAIL_IF_RECALL_BELOW =
    "Value between 0 (inclusive) and 100 (exclusive) indicating the recall tolerance percentage threshold before "
    "program fails with a non-zero exit code.  The default value of 0 means that the program will complete "
    "successfully with any recall value.  A non-zero value indicates the floor for acceptable recall values.  If the "
    "calculated recall value is below this threshold then the program will write out the results but return a non-zero "
    "exit code as a signal that the recall was not acceptable."; // does it continue running or die immediately?  Will I
                                                                 // still get my results even if the return code is -1?

const char *NUMBER_OF_NODES_TO_CACHE = "Number of BFS nodes around medoid(s) to cache.  Default value: 0";
const char *BEAMWIDTH = "Beamwidth for search. Set 0 to optimize internally.  Default value: 2";
const char *MAX_BUILD_DEGREE = "Maximum graph degree";
const char *GRAPH_BUILD_COMPLEXITY =
    "Size of the search working set during build time.  This is the numer of neighbor/distance pairs to keep in memory "
    "while building the index.  Higher value results in a higher quality graph but it will take more time to build the "
    "graph.";
const char *GRAPH_BUILD_ALPHA = "Alpha controls density and diameter of graph, set 1 for sparse graph, 1.2 or 1.4 for "
                                "denser graphs with lower diameter";
const char *BUIlD_GRAPH_PQ_BYTES = "Number of PQ bytes to build the index; 0 for full precision build";
const char *USE_OPQ = "Use Optimized Product Quantization (OPQ).";
const char *LABEL_FILE = "Input label file in txt format for Filtered Index build. The file should contain comma "
                         "separated filters for each node with each line corresponding to a graph node";
const char *UNIVERSAL_LABEL =
    "Universal label, Use only in conjunction with label file for filtered index build. If a "
    "graph node has all the labels against it, we can assign a special universal filter to the "
    "point instead of comma separated filters for that point.  The universal label should be assigned to nodes "
    "in the labels file instead of listing all labels for a node.  DiskANN will not automatically assign a "
    "universal label to a node.";
const char *FILTERED_LBUILD = "Build complexity for filtered points, higher value results in better graphs";

} // namespace program_options_utils
