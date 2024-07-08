copy into partsupp
from @${stageName}('${prefix}/partsupp.tbl*')
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');