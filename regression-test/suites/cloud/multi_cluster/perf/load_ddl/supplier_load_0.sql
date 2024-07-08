copy into supplier_0
from @${stageName}('${prefix}/supplier.tbl')
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');
