// 3 rows and 1 column
def rows = [3, 1, 10]
order_qt_select_union_all1 """
            select c1
            from
            (
              ${selectUnionAll(rows)}
            ) a
            """

// 3 rows and 2 columns
rows = [[1, "123"], [2, null], [0, "abc"]]
order_qt_select_union_all2 """
             select c1, c2
             from
             (
                ${selectUnionAll(rows)}
             ) b
             """