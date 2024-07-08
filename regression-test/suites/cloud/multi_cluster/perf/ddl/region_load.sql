copy into region
from @${stageName}('${prefix}/region.tbl')
properties ('file.type' = 'csv', 'file.column_separator' = '|', 'copy.async' = 'false');
