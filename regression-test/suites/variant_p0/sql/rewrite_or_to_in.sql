set rewrite_or_to_in_predicate_threshold = 2;
select
    cast(v["type"] as string), cast(v["repo"]["name"] as string), cast(v['created_at'] as string)
from ghdata
where cast(v["type"] as string) = 'Delete' or cast(v["repo"]["name"] as string) = 'megan777/calculator' or cast(v["created_as"] as string) = '2021-01-02T16:37:26Z'
order by k limit 10;