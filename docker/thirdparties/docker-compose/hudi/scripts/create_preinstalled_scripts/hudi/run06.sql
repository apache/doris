-- Insert demo data into tables
USE regression_hudi;

INSERT OVERWRITE TABLE user_activity_log_cow_partition VALUES
  (1, 1710000000000, 'login', '2024-03-01'),
  (2, 1710000001000, 'click', '2024-03-01'),
  (3, 1710000002000, 'logout', '2024-03-02');

INSERT OVERWRITE TABLE user_activity_log_cow_non_partition VALUES
  (1, 1710000000000, 'login'),
  (2, 1710000001000, 'click'),
  (3, 1710000002000, 'logout');

INSERT OVERWRITE TABLE user_activity_log_mor_partition VALUES
  (1, 1710000000000, 'login', '2024-03-01'),
  (2, 1710000001000, 'click', '2024-03-01'),
  (3, 1710000002000, 'logout', '2024-03-02');

INSERT OVERWRITE TABLE user_activity_log_mor_non_partition VALUES
  (1, 1710000000000, 'login'),
  (2, 1710000001000, 'click'),
  (3, 1710000002000, 'logout');

