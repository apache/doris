SELECT /*+SET_VAR(enable_fallback_to_original_planner=false) */ count() FROM github_events WHERE cast(v["type"] as string) = 'IssueCommentEvent'
