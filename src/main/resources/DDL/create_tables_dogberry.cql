CREATE KEYSPACE IF NOT EXISTS dogberry WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE dogberry;

DROP TABLE IF EXISTS dogberry.item_state;

CREATE TABLE item_state (
   id text PRIMARY KEY,
   entity text,
   checksum text,
   created timestamp,
   updated timestamp,
   action text,
   value text
   ) WITH comment = 'The primary table used to store the last known state for a given item.'
   ;
