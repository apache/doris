suite("rfv2") {
    sql """
    drop table if exists a;
    create table a
    (
        a1 int,
        a2 int,
        a3 varchar
    ) engine=olap
    duplicate key (a1)
    distributed by hash(a1) buckets 3
    properties('replication_num'='1');

    insert into a values(1, 2, 3), (1, 2, 3), (4, 5, 6), (7,8,9),(10, 11, 12);

    drop table if exists b;
    create table b
    (
        b1 int,
        b2 int,
        b3 varchar
    ) engine=olap
    duplicate key (b1)
    distributed by hash(b1) buckets 3
    properties('replication_num'='1');

    insert into b values (1, 2, 3);

    drop table if exists c;
    create table c
    (
        c1 int,
        c2 int,
        c3 varchar
    ) engine=olap
    duplicate key (c1)
    distributed by hash(c1) buckets 3
    properties('replication_num'='1');

    insert into c values (7,8,9);

    set runtime_filter_type=3;
    """

    qt_1 """
    explain shape plan
    select * from ((select a1, a2, a3 from a) intersect (select b1, b2, b3 from b) intersect (select c1, c2, c3 from c)) t;
    """
    
    qt_2 """
    explain shape plan
    select * from ((select a1+1 as x, a2, a3 from a) intersect (select b1, b2, b3 from b)) t;
    """

    qt_3 """
    explain shape plan
    select * from (
        (select a1+1 as x, a2, a3 from a) 
        intersect 
        (select b1, b2, b3 from b intersect select c1, c2, c3 from c)
        ) t;
    """
}