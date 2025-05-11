suite("topn_lazy_on_data_model") {
    // mor unique key user_id is not lazy materialized
    sql """
        drop table if exists mor;
        CREATE TABLE mor
        (
        `user_id` LARGEINT NOT NULL,
        `username` VARCHAR(50) NOT NULL,
        age int
        )
        UNIQUE KEY(user_id, username)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "false"
        );

        insert into mor values ( 1, 'a', 10),(1,'b', 20);
    """

    qt_shape "explain shape plan select * from mor order by username limit 1"


    // mow unique key user_id is lazy materialized
    sql """
        drop table if exists mow;
        CREATE TABLE IF NOT EXISTS mow
        (
        `user_id` LARGEINT NOT NULL,
        `username` VARCHAR(50) NOT NULL,
        age int
        )
        UNIQUE KEY(user_id, username)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1",
        "enable_unique_key_merge_on_write" = "true"
        );

        insert into mow values ( 1, 'a', 10),(1,'b', 20);
    """
    
    qt_shape "explain shape plan select *  from mow order by username limit 1"

    // agg key user_id is lazy materialized
    sql """
        drop table if exists agg; 
        CREATE TABLE IF NOT EXISTS agg
        (
        `user_id` LARGEINT NOT NULL,
        `username` VARCHAR(50) NOT NULL,
        age int REPLACE
        )
        aggregate KEY(user_id, username)
        DISTRIBUTED BY HASH(`user_id`) BUCKETS 1
        PROPERTIES (
        "replication_allocation" = "tag.location.default: 1"
        );
     insert into agg values ( 1, 'a', 10),(1,'b', 20);
    """
    
    qt_shape "explain shape plan select *  from agg order by username limit 1"
}