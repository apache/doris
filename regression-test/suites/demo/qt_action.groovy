/**
 * qt_xxx sql equals to quickTest(xxx, sql) witch xxx is tag.
 * the result will be compare to the relate file: ${DORIS_HOME}/regression_test/data/qt_action.out.
 *
 * if you want to generate .out tsv file for real execute result. you can run with -genOut or -forceGenOut option.
 * e.g
 *   ${DORIS_HOME}/run-regression-test.sh --run qt_action -genOut
 *   ${DORIS_HOME}/run-regression-test.sh --run qt_action -forceGenOut
 */
qt_select "select 1, 'beijing' union all select 2, 'shanghai'"

qt_select2 "select 2"

// order result by string dict then compare to .out file.
// order_qt_xxx sql equals to quickTest(xxx, sql, true).
order_qt_union_all  """
                select 2
                union all
                select 1
                union all
                select null
                union all
                select 15
                union all
                select 3
                """