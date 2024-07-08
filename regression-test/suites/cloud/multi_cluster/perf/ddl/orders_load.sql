copy into orders
from @${stageName}('${prefix}/orders.tbl*')
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');