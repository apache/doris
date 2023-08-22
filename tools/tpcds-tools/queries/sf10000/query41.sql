select  distinct(i_product_name)
 from item i1
 where i_manufact_id between 970 and 970+40 
   and (select count(*) as item_cnt
        from item
        where (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'frosted' or i_color = 'rose') and 
        (i_units = 'Lb' or i_units = 'Gross') and
        (i_size = 'medium' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'chocolate' or i_color = 'black') and
        (i_units = 'Box' or i_units = 'Dram') and
        (i_size = 'economy' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'slate' or i_color = 'magenta') and
        (i_units = 'Carton' or i_units = 'Bundle') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'cornflower' or i_color = 'firebrick') and
        (i_units = 'Pound' or i_units = 'Oz') and
        (i_size = 'medium' or i_size = 'large')
        ))) or
       (i_manufact = i1.i_manufact and
        ((i_category = 'Women' and 
        (i_color = 'almond' or i_color = 'steel') and 
        (i_units = 'Tsp' or i_units = 'Case') and
        (i_size = 'medium' or i_size = 'large')
        ) or
        (i_category = 'Women' and
        (i_color = 'purple' or i_color = 'aquamarine') and
        (i_units = 'Bunch' or i_units = 'Gram') and
        (i_size = 'economy' or i_size = 'petite')
        ) or
        (i_category = 'Men' and
        (i_color = 'lavender' or i_color = 'papaya') and
        (i_units = 'Pallet' or i_units = 'Cup') and
        (i_size = 'N/A' or i_size = 'small')
        ) or
        (i_category = 'Men' and
        (i_color = 'maroon' or i_color = 'cyan') and
        (i_units = 'Each' or i_units = 'N/A') and
        (i_size = 'medium' or i_size = 'large')
        )))) > 0
 order by i_product_name
 limit 100;
