select s_grade, s_class, s_math_score,
       min(coalesce(s_math_score,0)) over (partition by s_grade, s_class) as s_min_math,
       avg(coalesce(s_math_score, 0)) over (partition by s_grade, s_class) as s_avg_math
from tpch_tiny_scores
where s_math_score is not null;