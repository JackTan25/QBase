SET enable_seqscan = off;

CREATE TABLE t (val vector(2));
insert into t values('[1,2]'),('[3,4]'),('[5,6]'),('[7,8]'),('[9,10]');
CREATE INDEX ON t USING m3v (val vector_cosine_ops);
SELECT * FROM t ORDER BY val <=> '[9,10]' limit 5;
INSERT INTO t (val) VALUES ('[11,12]');
SELECT * FROM t ORDER BY val <=> '[9,10]' limit 5;
SELECT * FROM t ORDER BY val <=> '[11,12]' limit 5;
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> '[3,4]' limit 5;) t2;
SELECT COUNT(*) FROM (SELECT * FROM t ORDER BY val <=> (SELECT NULL::vector)) t2;
DROP TABLE t;
