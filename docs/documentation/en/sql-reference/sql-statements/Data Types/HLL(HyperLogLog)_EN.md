#HLL (Hyloglog)
## Description
MARKETING (M)
A variable length string, M represents the length of a variable length string. The range of M is 1-16385.
Users do not need to specify length and default values. Length is controlled within the system according to the aggregation degree of data
And HLL columns can only be queried or used by matching hll_union_agg, hll_raw_agg, hll_cardinality, hll_hash.

##keyword
High loglog, hll, hyloglog
