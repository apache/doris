select avg(p_retailprice), p_mfgr from part group by 2 order by count(*) limit 20
