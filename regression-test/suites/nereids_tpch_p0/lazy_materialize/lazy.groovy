suite("lazy"){

    sql """
        use regression_test_nereids_tpch_p0;
        set disable_join_reorder=true;
        set runtime_filter_mode=OFF;
        set enable_lazy_materialization=true;
    """

    qt_lazy_base """
        explain shape plan
        select l_partkey
        from lineitem
        order by l_orderkey
        limit 1;"""

    // can not perform lazy materialization on agg
    qt_agg """
    explain shape plan
    select  a 
    from (select sum(n_nationkey), n_regionkey as a from nation group by n_regionkey having n_regionkey>2) T;
    """

    qt_join """
    explain shape plan 
    select l_partkey, o_custkey, o_clerk
    from
    lineitem join orders on l_orderkey=o_orderkey
    where O_CUSTKEY < 100
    order by l_orderkey limit 1;
    """

    qt_2_topn_1 """
    explain shape plan
    select c_custkey, c_phone,n_comment
    from (
        select n_nationkey, n_comment from nation order by n_name limit 1
    ) T join customer on n_nationkey=c_nationkey
    order by c_name limit 1;
    """

    qt_2_topn_2 """
    explain shape plan
    select c_custkey, c_phone
    from (
        select n_nationkey from nation order by n_nationkey limit 1
    ) T join customer on n_nationkey=c_nationkey
    order by c_name limit 1;
    """
}