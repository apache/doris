copy into part
from @${stageName}('${prefix}/part.tbl')
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');