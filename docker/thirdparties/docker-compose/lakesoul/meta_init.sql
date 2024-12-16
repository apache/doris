-- SPDX-FileCopyrightText: 2023 LakeSoul Contributors
--
-- SPDX-License-Identifier: Apache-2.0

create table if not exists namespace
(
    namespace  text,
    properties json,
    comment    text,
    domain     text default 'public',
    primary key (namespace)
);

insert into namespace(namespace, properties, comment) values ('default', '{}', '')
ON CONFLICT DO NOTHING;

create table if not exists table_info
(
    table_id        text,
    table_namespace text default 'default',
    table_name      text,
    table_path      text,
    table_schema    text,
    properties      json,
    partitions      text,
    domain          text default 'public',
    primary key (table_id)
);

create table if not exists table_name_id
(
    table_name      text,
    table_id        text,
    table_namespace text default 'default',
    domain          text default 'public',
    primary key (table_name, table_namespace)
);

create table if not exists table_path_id
(
    table_path      text,
    table_id        text,
    table_namespace text default 'default',
    domain          text default 'public',
    primary key (table_path)
);

DO
$$
    BEGIN
        IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'data_file_op') THEN
            create type data_file_op as
            (
                path            text,
                file_op         text,
                size            bigint,
                file_exist_cols text
            );
        END IF;
    END
$$;

create table if not exists data_commit_info
(
    table_id       text,
    partition_desc text,
    commit_id      UUID,
    file_ops       data_file_op[],
    commit_op      text,
    committed      boolean default 'false',
    timestamp      bigint,
    domain         text default 'public',
    primary key (table_id, partition_desc, commit_id)
);

create table if not exists partition_info
(
    table_id       text,
    partition_desc text,
    version        int,
    commit_op      text,
    timestamp      bigint DEFAULT (date_part('epoch'::text, now()) * (1000)::double precision),
    snapshot       UUID[],
    expression     text,
    domain         text default 'public',
    primary key (table_id, partition_desc, version)
);

CREATE OR REPLACE FUNCTION partition_insert() RETURNS TRIGGER AS
$$
DECLARE
    rs_version         integer;
    rs_table_path      text;
    rs_table_namespace text;
BEGIN
    if NEW.commit_op <> 'CompactionCommit' then
        select version
        INTO rs_version
        from partition_info
        where table_id = NEW.table_id
          and partition_desc = NEW.partition_desc
          and version != NEW.version
          and commit_op = 'CompactionCommit'
        order by version desc
        limit 1;
        if rs_version >= 0 then
            if NEW.version - rs_version >= 10 then
                select table_path, table_namespace
                into rs_table_path, rs_table_namespace
                from table_info
                where table_id = NEW.table_id;
                perform pg_notify('lakesoul_compaction_notify',
                                  concat('{"table_path":"', rs_table_path, '","table_partition_desc":"',
                                         NEW.partition_desc, '","table_namespace":"', rs_table_namespace, '"}'));
            end if;
        else
            if NEW.version >= 10 then
                select table_path, table_namespace
                into rs_table_path, rs_table_namespace
                from table_info
                where table_id = NEW.table_id;
                perform pg_notify('lakesoul_compaction_notify',
                                  concat('{"table_path":"', rs_table_path, '","table_partition_desc":"',
                                         NEW.partition_desc, '","table_namespace":"', rs_table_namespace, '"}'));
            end if;
        end if;
        RETURN NULL;
    end if;
    RETURN NULL;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER partition_table_change
    AFTER INSERT
    ON partition_info
    FOR EACH ROW
EXECUTE PROCEDURE partition_insert();

create table if not exists global_config
(
    key  text,
    value text,
    primary key (key)
);
