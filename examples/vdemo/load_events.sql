insert into event(id, user_id, tenant_id, app, spent, other, event_time) values
  (5, 1, 1, 'ps1', 13, 'Vitess1', unix_timestamp()),
  (6, 1, 1, 'ps1', 17, 'Vitess1', unix_timestamp()),
  (7, 2, 1, 'ps1', 100, 'Vitess1', unix_timestamp()),
  (8, 2, 1, 'ps1', 101, 'Vitess1', unix_timestamp()),
  (9, 2, 1, 'ps3', 101, 'Vitess1', unix_timestamp()),
  (10, 3, 1, 'ps3', 101, 'Vitess1', unix_timestamp()),
  (11, 4, 1, 'ps3', 101, 'Vitess1', unix_timestamp()),
  (12, 5, 1, 'ps3', 101, 'Vitess1', unix_timestamp());
