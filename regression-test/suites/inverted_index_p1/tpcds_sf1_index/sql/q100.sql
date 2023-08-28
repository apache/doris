SELECT 
    t1.web_name, 
    COUNT(t1.web_name) AS count 
FROM 
    web_site t1 
LEFT JOIN 
    web_sales t2 ON t1.web_site_sk = t2.ws_web_site_sk 
WHERE 
    t1.web_name MATCH 'site_0' 
    OR t2.ws_item_sk > 17934 
GROUP BY 
    t1.web_name 
ORDER BY 
    t1.web_name;
