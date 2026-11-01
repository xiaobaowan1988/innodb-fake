INSTALL PLUGIN myengine SONAME 'ha_myengine.so';
CREATE DATABASE IF NOT EXISTS t; USE t;
CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=MyEngine;
INSERT INTO t1 VALUES (1);
SELECT * FROM t1 WHERE id = 1;   -- Empty set
SELECT * FROM t1;                -- Empty set
