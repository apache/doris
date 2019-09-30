# CREATE VIEW
## Description
    This statement is used to create a logical view
    Grammar:
    
        CREATE VIEW [IF NOT EXISTS]
        [db_name.]view_name
        (column1[ COMMENT "col comment"][, column2, ...])
        AS query_stmt

    Explain:

        1. Views are logical views without physical storage. All queries on views are equivalent to sub-queries corresponding to views.
        2. query_stmt is arbitrarily supported SQL.

## example

    1. Create view example_view on example_db

        CREATE VIEW example_db.example_view (k1, k2, k3, v1)
        AS
        SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
        WHERE k1 = 20160112 GROUP BY k1,k2,k3;
        
    2. Create view with comment
    
        CREATE VIEW example_db.example_view
        (
            k1 COMMENT "first key",
            k2 COMMENT "second key",
            k3 COMMENT "third key",
            v1 COMMENT "first value"
        )
        COMMENT "my first view"
        AS
        SELECT c1 as k1, k2, k3, SUM(v1) FROM example_table
        WHERE k1 = 20160112 GROUP BY k1,k2,k3;

## keyword

    CREATE,VIEW

