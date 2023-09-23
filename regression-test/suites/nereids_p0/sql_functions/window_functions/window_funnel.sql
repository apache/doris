DROP TABLE IF EXISTS windowfunnel_test;

CREATE TABLE IF NOT EXISTS windowfunnel_test (
                `xwho` varchar(50) NULL COMMENT 'xwho', 
                `xwhen` datetime COMMENT 'xwhen', 
                `xwhat` int NULL COMMENT 'xwhat' 
                )
DUPLICATE KEY(xwho) 
DISTRIBUTED BY HASH(xwho) BUCKETS 3 
PROPERTIES ( 
    "replication_num" = "1"
); 

INSERT into windowfunnel_test (xwho, xwhen, xwhat) values ('1', '2022-03-12 10:41:00', 1),
                                                   ('1', '2022-03-12 13:28:02', 2),
                                                   ('1', '2022-03-12 16:15:01', 3),
                                                   ('1', '2022-03-12 19:05:04', 4); 

select * from windowfunnel_test;

select window_funnel(1, 'default', t.xwhen, t.xwhat = 1, t.xwhat = 2 ) AS level from windowfunnel_test t;
select window_funnel(3600 * 3, 'default', t.xwhen, t.xwhat = 1, t.xwhat = 2 ) AS level from windowfunnel_test t;


CREATE TABLE IF NOT EXISTS user_analysis
(
    user_id INT NOT NULL ,
    event_type  varchar(20) ,
    event_time datetime NOT NULL 
)
DUPLICATE KEY(`user_id`, `event_type`)
DISTRIBUTED BY HASH(`event_type`) BUCKETS 2
PROPERTIES (
    "replication_num" = "1"
);


insert into user_analysis values (1000001,'browse', '2022-07-17 00:00:00');
insert into user_analysis values (1000002,'browse','2022-07-18 00:00:00');
insert into user_analysis values (1000003,'shopping cart','2022-07-19 00:00:00');
insert into user_analysis values (1000004,'browse','2022-07-20 00:00:00');
insert into user_analysis values (1000005,'browse','2022-07-21 00:00:00');
insert into user_analysis values (1000006,'favorite','2022-07-22 00:00:00');
insert into user_analysis values (1000007,'browse','2022-07-23 00:00:00');
insert into user_analysis values (1000008,'browse','2022-07-23 23:31:00');
insert into user_analysis values (1000008,'favorite','2022-07-23 23:50:00');
insert into user_analysis values (1000008,'shopping cart','2022-07-23 23:58:00');
insert into user_analysis values (1000008,'buy','2022-07-24 00:00:00');
insert into user_analysis values (1000009,'browse','2022-07-25 00:00:00');
insert into user_analysis values (1000010,'favorite','2022-07-26 00:00:00');
insert into user_analysis values (1000007,'browse','2022-07-27 00:00:00');
insert into user_analysis values (1000012,'browse','2022-07-28 00:00:00');
insert into user_analysis values (1000013,'browse','2022-07-29 00:00:00');
insert into user_analysis values (1000014,'browse','2022-07-30 00:00:00');
insert into user_analysis values (1000015,'browse','2022-07-31 00:00:00');
--- Nereids does't support window function
--- WITH
---     level_detail AS ( 
---         SELECT 
---             level
---             ,COUNT(1) AS count_user 
---         FROM ( 
---             SELECT 
---                 user_id 
---                 ,window_funnel(
---                     1800
---                     ,'default'
---                     ,event_time
---                     ,event_type = 'browse'
---                     ,event_type = 'favorite'
---                     ,event_type = 'shopping cart'
---                     ,event_type = 'buy' 
---                     ) AS level 
---             FROM user_analysis
---             WHERE event_time >= TIMESTAMP '2022-07-17 00:00:00'
---                 AND event_time < TIMESTAMP '2022-07-31 00:00:00'
---             GROUP BY user_id 
---             ) AS basic_table 
---         GROUP BY level 
---         ORDER BY level ASC )
--- SELECT  CASE level    WHEN 0 THEN 'users'
---                       WHEN 1 THEN 'browser'
---                       WHEN 2 THEN 'favorite'
---                       WHEN 3 THEN 'shopping cart'
---                       WHEN 4 THEN 'buy' 
---               END
---         ,SUM(count_user) over ( ORDER BY level DESC )
--- FROM    level_detail
--- GROUP BY level
---          ,count_user
--- ORDER BY level ASC;


