SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */ count(distinct cast(v["repo"]["name"] as string)) FROM github_events
