suite("explain_action", "demo") {
    explain {
        sql("select k2 from left_outer_left_table a left outer join left_outer_right_table b on b.k2 = a.k1 where a.k1='uuid'")

        // contains("OUTPUT EXPRS:<slot 0> 100\n") && contains("PARTITION: UNPARTITIONED\n")
        contains "OUTPUT EXPRS:<slot 0> 100\n"
        contains "PARTITION: UNPARTITIONED\n"
    }
} 
