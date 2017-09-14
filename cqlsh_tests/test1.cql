CREATE KEYSPACE IF NOT EXISTS source_cmd_test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
CREATE TABLE IF NOT EXISTS source_cmd_test.tbl ( a int PRIMARY KEY, b int);
SELECT * FROM source_cmd_test.tbl;
SOURCE 'cqlsh_tests/test2.cql';
