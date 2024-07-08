copy into supplier
from @${stageName}('${prefix}/supplier.tbl')
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');
