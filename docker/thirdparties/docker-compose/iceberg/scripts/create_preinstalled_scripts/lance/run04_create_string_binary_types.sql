-- STRING and BINARY exercise empty values, Unicode, embedded zero bytes and NULLs.
DROP TABLE IF EXISTS lance.doris.string_binary_types;

CREATE TABLE lance.doris.string_binary_types (
    row_id BIGINT NOT NULL,
    text_value STRING,
    binary_value BINARY
) USING lance;

INSERT INTO lance.doris.string_binary_types VALUES
    (1, '', X''),
    (2, 'Doris 与 Lance ��', X'0001FF7F');

INSERT INTO lance.doris.string_binary_types VALUES
    (3, NULL, NULL),
    (4, 'a moderately long text value used to exercise variable-length Arrow buffers',
     X'6C616E6365');
