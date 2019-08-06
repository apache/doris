# SHOW ROLES
## description
## description
    该语句用于展示所有已创建的角色信息，包括角色名称，包含的用户以及权限。

    语法：
        SHOW ROLES;

    该语句用户删除一个角色
    
    语法：
        DROP ROLE role1;
        
    删除一个角色，不会影响之前属于该角色的用户的权限。仅相当于将该角色与用户解耦。用户已经从该角色中获取到的权限，不会改变。
    
## example

    1. 查看已创建的角色：

        SHOW ROLES;

## keyword
    SHOW,ROLES

## example

    1. 删除一个角色
   
        DROP ROLE role1;
        
## keyword
   DROP, ROLE     

