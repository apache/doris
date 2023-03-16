select count() from test_nbagames_json;
select max(`teams.results.orb`[1]) from test_nbagames_json;
select sum(cast(element_at(`teams.results.ft_pct`, 1) as double)) from test_nbagames_json;
select sum(cast(`teams.results.ft_pct`[1] as double)) from test_nbagames_json where size(`teams.results.ft_pct`) = 2;
select sum(cast(`teams.results.fg3a`[1] as int)) from test_nbagames_json;
select sum(cast(`teams.results.fg3a`[2] as int)) from test_nbagames_json;
select `teams.results.ft_pct` from test_nbagames_json where `teams.results.ft_pct`[1] = ".805";
select sum(`teams.home`[1]) from test_nbagames_json;
select `teams.results.ft_pct` from test_nbagames_json where `teams.results.ft_pct`[1] = ".805" order by `teams.results.ft_pct`[2];
