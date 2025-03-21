use `default`;


CREATE TABLE json_nested_complex_table (
    user_ID STRING,
    user_PROFILE STRUCT<
        name: STRING,
        AGE: INT,
        preferences: MAP<
            STRING,
            STRUCT<
                preference_ID: INT,
                preference_VALUES: ARRAY<STRING>
            >
        >
    >,
    activity_LOG ARRAY<
        STRUCT<
            activity_DATE: STRING,
            activities: MAP<
                STRING,
                STRUCT<
                    `DETAILS`: STRING,
                    metrics: MAP<STRING, float>
                >
            >
        >
    >
) ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'

LOCATION
  '/user/doris/preinstalled_data/json/json_nested_complex_table';


msck repair table json_nested_complex_table;
