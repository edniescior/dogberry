CREATE KEYSPACE IF NOT EXISTS test WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': '1'
};

USE test;

CREATE TABLE IF NOT EXISTS test.users (
  userid text PRIMARY KEY,
  first_name text,
  last_name text,
  epoch_val timestamp
);
