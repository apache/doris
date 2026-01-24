suite("test_gamma") {
    sql "set debug_skip_fold_constant=false"
    qt_select_const "select gamma(5), gamma(0.5), gamma(1)"
    qt_select_null "select gamma(null)"
    qt_select_zero "select gamma(0)"
    qt_select_neg "select gamma(-1)"

    sql "set debug_skip_fold_constant=true"
    qt_select_const_no_fold "select gamma(5), gamma(0.5), gamma(1)"
    qt_select_null_no_fold "select gamma(null)"
    qt_select_zero_no_fold "select gamma(0)"
    qt_select_neg_no_fold "select gamma(-1)"

    sql "drop table if exists test_gamma_tbl"
    sql """
        create table test_gamma_tbl (
            k1 int,
            v1 double
        ) distributed by hash(k1) properties("replication_num" = "1");
    """

    sql """
        insert into test_gamma_tbl values 
        (1, 1.0),
        (2, 2.0),
        (3, 3.0),
        (4, 4.0),
        (5, 5.0),
        (6, 0.5),
        (7, 0.0),
        (8, -1.0),
        (9, null);
    """

    qt_select_table "select k1, v1, gamma(v1) from test_gamma_tbl order by k1"

    sql "drop table test_gamma_tbl"
}
