select 2 from nation group by 1;
select "oneline" from nation group by "I am a const, but not to be removed";
select a from (select "abc" as a, '1' as b) T  group by b, a;

