# DROP ROLE
## description
    该语句用户删除一个角色
    
    语法：
        DROP ROLE role1;
        
    删除一个角色，不会影响之前属于该角色的用户的权限。仅相当于将该角色与用户解耦。用户已经从该角色中获取到的权限，不会改变。
    
## example

    1. 删除一个角色
   
        DROP ROLE role1;
        
## keyword
   DROP, ROLE     

