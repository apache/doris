-- Test Case 1: Basic match (3 lowercase words with spaces)
SELECT regexp_count('1a 2b 14m', '\s*[a-z]+\s*') AS result;

-- Test Case 2: All letters (2 words)
SELECT regexp_count('hello world', '\s*[a-z]+\s*') AS result;

-- Test Case 3: Single letters (3 matches)
SELECT regexp_count('a b c', '\s*[a-z]+\s*') AS result; 

-- Test Case 4: Uppercase letters (no matches)
SELECT regexp_count('A B C', '\s*[a-z]+\s*') AS result;

-- Test Case 5: Letter-number combinations (3 matches)
SELECT regexp_count('a1 b2 c3', '\s*[a-z]+\d\s*') AS result;

-- Test Case 6: Multiple spaces (3 matches)
SELECT regexp_count('a  b   c', '\s+[a-z]+\s+') AS result; 

-- Test Case 7: Single letter without spaces (1 match)
SELECT regexp_count('a', '\s*[a-z]+\s*') AS result; 

-- Test Case 8: Newline/tab separators (3 matches)
SELECT regexp_count('a\nc\tb', '\s*[a-z]+\s*') AS result; 

-- Test Case 9: No letters (0 matches)
SELECT regexp_count('123', '\s*[a-z]+\s*') AS result; 

-- Test Case 10: Empty string (0 matches)
SELECT regexp_count('', '\s*[a-z]+\s*') AS result;

-- Test Case 11: Whitespace only (0 matches)
SELECT regexp_count('   ', '\s*[a-z]+\s*') AS result; 
-- Test Case 12: Trailing whitespace (1 match)
SELECT regexp_count('a   ', '\s*[a-z]+\s*') AS result;

-- Test Case 13: Leading whitespace (1 match)
SELECT regexp_count('   a', '\s*[a-z]+\s*') AS result;

-- Test Case 14: Letters with special characters (2 matches)
SELECT regexp_count('ab-cd_ef', '\s*[a-z]+[^a-z]\s*') AS result;

-- Test Case 15: Consecutive letters and numbers (1 match)
SELECT regexp_count('xyz123xyz', '\s*[a-z]+\d+\s*') AS result;

-- Test Case 16: Words with length ≥3 (1 match)
SELECT regexp_count('longword', '\s*[a-z]{3,}\s*') AS result; 

-- Test Case 17: Words with length =2 (2 matches)
SELECT regexp_count('ab cd', '\s*[a-z]{2}\s*') AS result; 

-- Test Case 18: Letters surrounded by special characters (3 matches)
SELECT regexp_count('!@#a$%b^&c', '\s*[a-z]+\s*') AS result;

-- Test Case 19: Mixed newline characters (3 matches)
SELECT regexp_count('a\nb\tc\r', '\s*[a-z]+\s*') AS result; 

-- Test Case 20: String "NULL" (0 matches, uppercase)
SELECT regexp_count('NULL', '\s*[a-z]+\s*') AS result;