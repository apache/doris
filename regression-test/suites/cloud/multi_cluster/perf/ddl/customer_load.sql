copy into customer
from @${stageName}('${prefix}/customer.tbl')
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');