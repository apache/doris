-- SPDX-FileCopyrightText: 2023 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

delete from namespace;
insert into namespace(namespace, properties, comment) values ('default', '{}', '');
delete from data_commit_info;
delete from table_info;
delete from table_path_id;
delete from table_name_id;
delete from partition_info;
