CREATE KEYSPACE IF NOT EXISTS test WITH replication = {
  'class': 'SimpleStrategy',
  'replication_factor': '1'
};

USE test;

DROP TABLE IF EXISTS test.eav;

CREATE TABLE test.eav (
   ent text,
   attr text,
   value text,
   PRIMARY KEY (ent, attr)
   ) WITH comment = 'This table models the (lamentably) common Entity-Attribute-Value (EAV) pattern.'
   ;
