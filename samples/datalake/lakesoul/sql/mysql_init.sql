-- SPDX-FileCopyrightText: 2023 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

create database if not exists test_cdc;

use test_cdc;

create table `default_init`
(
    `id`     int            NOT NULL,
    `col_1`  bigint         NOT NULL,
    `col_2`  binary(1)      NOT NULL,
    `col_3`  blob           NOT NULL,
    `col_4`  char(1)        NOT NULL,
    `col_5`  date           NOT NULL,
    `col_6`  datetime       NOT NULL,
    `col_7`  decimal(10, 2) NOT NULL,
    `col_8`  double         NOT NULL,
    `col_9`  enum('spring','summer','autumn','winter') NOT NULL,
    `col_10` int            NOT NULL,
    `col_11` longblob       NOT NULL,
    `col_12` longtext       NOT NULL,
    `col_13` mediumblob     NOT NULL,
    `col_14` mediumint      NOT NULL,
    `col_15` mediumtext     NOT NULL,
    `col_16` set('first','second','third','fourth','fifth') NOT NULL,
    `col_17` smallint       NOT NULL,
    `col_18` text           NOT NULL,
    `col_19` timestamp      NOT NULL,
    `col_20` tinyblob       NOT NULL,
    `col_21` tinyint        NOT NULL,
    `col_22` tinytext       NOT NULL,
    `col_23` varbinary(50) NOT NULL,
    `col_24` varchar(50)    NOT NULL,
    PRIMARY KEY (`id`)
);

create table `default_init_1`
(
    `id`     int            NOT NULL,
    `col_1`  bigint         NOT NULL,
    `col_2`  binary(1)      NOT NULL,
    `col_3`  blob           NOT NULL,
    `col_4`  char(1)        NOT NULL,
    `col_5`  date           NOT NULL,
    `col_7`  decimal(10, 2) NOT NULL,
    `col_8`  double         NOT NULL,
    `col_9`  enum('spring','summer','autumn','winter') NOT NULL,
    `col_10` int            NOT NULL,
    `col_11` longblob       NOT NULL,
    `col_12` longtext       NOT NULL,
    `col_13` mediumblob     NOT NULL,
    `col_14` mediumint      NOT NULL,
    `col_15` mediumtext     NOT NULL,
    `col_16` set('first','second','third','fourth','fifth') NOT NULL,
    `col_18` text           NOT NULL,
    `col_20` tinyblob       NOT NULL,
    `col_22` tinytext       NOT NULL,
    `col_23` varbinary(50) NOT NULL,
    `col_24` varchar(50)    NOT NULL,
    PRIMARY KEY (`id`)
);

insert into default_init values (1, 8132132390403693530, 'N', 'blob', 'd', '2023-03-10', '2023-03-10 07:00:00', 8.20, 8.212, 'spring', 10, 'longblob', 'longtext', 'mediumblob', 83886, 'mediumtext', 'second', '99', 'text', '2023-03-10 07:00:00', 'tinyblob', 9, 'tinytext', 'varbinary', 'varchar');

