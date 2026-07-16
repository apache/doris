-- This fixture is intentionally a Directory Namespace V2 catalog on MinIO.
-- It exercises catalog namespace/table discovery and leaves two committed writes.
CREATE NAMESPACE IF NOT EXISTS lance.doris;

DROP TABLE IF EXISTS lance.doris.user_profiles;

CREATE TABLE lance.doris.user_profiles (
    user_id BIGINT NOT NULL,
    user_name STRING,
    engagement_score DOUBLE,
    interests ARRAY<STRING>
) USING lance;

INSERT INTO lance.doris.user_profiles VALUES
    (1, 'alice', 98.5, array('engineering', 'lance')),
    (2, 'bob', 87.0, array('analytics'));

INSERT INTO lance.doris.user_profiles VALUES
    (3, 'carol', 91.25, array('engineering', 'spark')),
    (4, 'david', NULL, NULL);

SELECT * FROM lance.doris.user_profiles ORDER BY user_id;
