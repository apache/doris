SELECT * FROM articles_dk WHERE title MATCH_ANY 'Database' OR body MATCH_ANY 'Database' ORDER BY id;
SELECT COUNT(*) FROM articles_dk WHERE title MATCH_ANY 'database' OR body MATCH_ANY 'database';
SELECT * FROM articles_dk WHERE title MATCH_ANY 'Tutorial' OR body MATCH_ANY 'Tutorial' ORDER BY id;
SELECT * FROM articles_dk WHERE title MATCH_ANY 'MySQL' OR body MATCH_ANY 'MySQL' ORDER BY id;

-- NOT SUPPORT:
-- SELECT COUNT(IF(title MATCH_ANY 'database' OR body MATCH_ANY 'database', 1, NULL)) as count FROM articles_dk;

SELECT * FROM articles_dk WHERE (title MATCH_ANY 'MySQL' AND NOT title MATCH_ANY 'YourSQL') 
                            OR (body MATCH_ANY 'MySQL' AND NOT body MATCH_ANY 'YourSQL') ORDER BY id;

SELECT * FROM articles_dk WHERE title MATCH_ANY 'DBMS Security' OR body MATCH_ANY 'DBMS Security' ORDER BY id;
SELECT * FROM articles_dk WHERE title MATCH_ALL 'MySQL YourSQL' OR body MATCH_ALL 'MySQL YourSQL' ORDER BY id;

SELECT * FROM articles_dk WHERE (title MATCH_ANY 'MySQL' OR body MATCH_ANY 'MySQL') 
                            AND NOT (title MATCH_ANY 'Well stands' OR body MATCH_ANY 'Well stands') ORDER BY id;

SELECT * FROM articles_dk WHERE (title MATCH_ANY 'YourSQL' OR body MATCH_ANY 'YourSQL') 
                            OR ((title MATCH_ANY 'MySQL' OR body MATCH_ANY 'MySQL') 
                                AND NOT (title MATCH_ANY 'Tricks Security' OR body MATCH_ANY 'Tricks Security')) ORDER BY id;

SELECT * FROM articles_dk WHERE ((title MATCH_ANY 'MySQL' OR body MATCH_ANY 'MySQL') 
                                AND NOT (title MATCH_ANY 'Tricks Security' OR body MATCH_ANY 'Tricks Security'))
                           AND NOT (title MATCH_ANY 'YourSQL' OR body MATCH_ANY 'YourSQL') ORDER BY id;

-- match phrase, NOT SUPPORT:
-- SELECT * FROM articles_dk WHERE title MATCH_PHRASE 'following database' OR body MATCH_PHRASE 'following database';