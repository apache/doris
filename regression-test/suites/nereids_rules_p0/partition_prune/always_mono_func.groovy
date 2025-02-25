// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

suite("always_mono_func") {
    sql "drop table if exists always_mono_func"
    sql """create table always_mono_func (a int, dt datetime, d date, c varchar(100)) duplicate key(a)
    partition by range(dt) (
            partition p1 values less than ("2017-01-01"),
            partition p2 values less than ("2018-01-01"),
            partition p3 values less than ("2019-01-01"),
            partition p4 values less than ("2020-01-01"),
            partition p5 values less than ("2021-01-01")
    ) distributed by hash(a) properties("replication_num"="1");"""
    sql """INSERT INTO always_mono_func SELECT number,
    date_add('2016-01-01 00:00:00', interval number month),
    cast(date_add('2022-01-01 00:00:00', interval number month) as date), cast(number as varchar(65533)) FROM numbers('number'='55');"""
    sql "INSERT INTO always_mono_func  values(3,null,null,null);"

    explain {
        sql """select * from always_mono_func where date_format(dt, "yyyyMMdd") <"20190101" """
        contains("partitions=3/5 (p1,p2,p3)")
    }

    explain {
        sql """select * from always_mono_func where date_format(dt, "yyyyMMdd") <"2019-01-01" """
        contains("partitions=3/5 (p1,p2,p3)")
    }

    explain {
        sql """select * from always_mono_func where date_format(dt, "yyyy-MM-dd") < "2019-01-01" """
        contains("partitions=3/5 (p1,p2,p3)")
    }

    explain {
        sql """select * from always_mono_func where date_format(dt, "yyyy-MM-dd") > "2019-01-01" """
        contains("partitions=2/5 (p4,p5)")
    }

    explain {
        sql """select * from always_mono_func where date_format(dt, "yyyy-MM-dd HH:mm:ss") < "2019-01-01" """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "yyyy-MM-dd") = "2019-01-01" """
        contains("partitions=2/5 (p3,p4)")
    }

    explain {
        sql """select * from always_mono_func where date_format(dt, "yyyy-MM-dd HH:mm:ss") = "2019-01-01 00:00:00" """
        contains("partitions=2/5 (p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y") <= "2018" and date_format(dt, "%Y") > "2017" """
        contains("partitions=2/5 (p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y") <= "2019-01-01" """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y-%m")>="2019-01-01" """
        contains("partitions=2/5 (p4,p5)")
    }

    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y-%m-%d")<>"2019-01-01" """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y-%m-%d %H")<="2019-01-01" """
        contains("partitions=3/5 (p1,p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y-%m-%d %H:%i")> "2019-01-01" """
        contains("partitions=3/5 (p3,p4,p5)")
    }

    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y-%m-%d %H:%i:%s")> "2019-01-01 00:00:00" """
        contains("partitions=2/5 (p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y-%m-%d %H:%i:%S")> "2019-01-01 00:00:00" """
        contains("partitions=2/5 (p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y-%m-%d %T") > "2019-01-01" """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y%m") > "20190101" """
        contains("partitions=2/5 (p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y %m") > "2019 01" """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }

    explain {
        sql """select * from always_mono_func where date_format(dt, "yyyyddMM") <"20190101" """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }

    explain {
        sql """select * from always_mono_func where date_format(dt, "%Y-%d-%m %T") <"2019-01-01" """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }

    // year
    explain {
        sql """ select * from always_mono_func where year(dt) >= 2019 """
        contains("partitions=3/5 (p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where year(dt) < 2019 and year(dt) > 2017"""
        contains("partitions=2/5 (p2,p3)")
    }
    explain {
        sql """select * from always_mono_func where year(dt) <2023"""
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
    // to_monday
    explain {
        sql """select * from always_mono_func where to_monday(dt) <'2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where to_monday(dt) ='2019-01-01' """
        contains("partitions=2/5 (p1,p4)")
    }
    explain {
        sql """select * from always_mono_func where to_monday(dt) >='2018-01-01' and to_monday(dt) <'2019-01-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """ select * from always_mono_func where to_monday(dt) >= "2019-01-01" """
        contains("partitions=3/5 (p1,p4,p5)")
    }
    // to_date
    explain {
        sql """select * from always_mono_func where to_date(dt) <'2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where to_date(dt) <='2023-02-01' """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where to_date(dt) is null """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where to_date(dt) is not null """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where to_date(dt) >='2018-01-01' and to_date(dt) <'2019-01-01' """
        contains("partitions=2/5 (p2,p3)")
    }
    explain {
        sql """ select * from always_mono_func where to_date(dt) > "2019-01-01" """
        contains("partitions=2/5 (p4,p5)")
    }
    // last_day
    explain {
        sql """select * from always_mono_func where last_day(dt) <='2019-02-01' """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func where last_day(dt) <='2023-02-01' """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where last_day(dt) is null """
        contains("partitions=1/5 (p1)")
    }
    explain {
        sql """select * from always_mono_func where last_day(dt) is not null """
        contains("partitions=5/5 (p1,p2,p3,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func where last_day(dt) >='2018-01-01' and last_day(dt) <'2019-01-01' """
        contains("partitions=2/5 (p2,p3)")
    }
    explain {
        sql """ select * from always_mono_func where last_day(dt) > "2019-01-01" """
        contains("partitions=3/5 (p3,p4,p5)")
    }

    explain {
        sql """select * from always_mono_func  where date_format(to_monday(dt), 'yyyyMMdd') >= "20190101" """
        contains("partitions=3/5 (p1,p4,p5)")
    }
    explain {
        sql """select * from always_mono_func  where date_format(last_day(to_monday(dt)), 'yyyyMMdd') < "20190101" """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }
    explain {
        sql """select * from always_mono_func  where date_format(last_day(to_monday(to_date(dt))), 'yyyyMMdd') <= "20190101" """
        contains("partitions=4/5 (p1,p2,p3,p4)")
    }

    explain {
        sql """select * from always_mono_func  where date_format(date_trunc(last_day(to_monday(dt)),'month'), 'yyyyMMdd') > "20190101" """
        contains("partitions=3/5 (p1,p4,p5)")
    }
}