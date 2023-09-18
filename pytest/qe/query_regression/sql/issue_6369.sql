--https://github.com/apache/incubator-doris/issues/6369
SELECT ROUTINE_SCHEMA, ROUTINE_NAME FROM information_schema.ROUTINES WHERE ROUTINE_SCHEMA = 'ech_dw' ORDER BY ROUTINES.ROUTINE_SCHEMA;
