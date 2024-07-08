copy into nation
from @${stageName}('${prefix}/nation.tbl')
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');