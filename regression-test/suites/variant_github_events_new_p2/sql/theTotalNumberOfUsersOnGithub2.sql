SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */ count(distinct cast(v["actor"]["login"] as string)) FROM github_events WHERE cast(v["type"] as string) = 'WatchEvent'
