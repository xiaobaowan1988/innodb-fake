-- 在 INSTALL PLUGIN 之后执行（scripts/restart.sh 已完成安装）
CREATE DATABASE IF NOT EXISTS t; USE t;
DROP TABLE IF EXISTS t1;
CREATE TABLE t1 (id INT PRIMARY KEY) ENGINE=MyEngine;
INSERT INTO t1 VALUES (1),(2),(3);
SELECT * FROM t1 WHERE id = 1;        -- 1 行
SELECT * FROM t1 WHERE id > 1;        -- 2 行
SELECT * FROM t1 ORDER BY id DESC;    -- 3,2,1
DELETE FROM t1 WHERE id = 2;
SELECT COUNT(*) FROM t1;              -- 2
