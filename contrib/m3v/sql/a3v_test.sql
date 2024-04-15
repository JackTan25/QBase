-- we reuse vector's vector type here.
create extension if not exists vector;
create extension if not exists m3v;
drop table if exists t1;
create table t1(a vector(2),b vector(3),c vector(4));
insert into t1 values('[1,1]','[1,1,1]','[1,1,1,1]');
insert into t1 values('[2,2]','[2,2,2]','[2,2,2,2]');
insert into t1 values('[3,3]','[3,3,3]','[3,3,3,3]');
insert into t1 values('[4,4]','[4,4,4]','[4,4,4,4]');
create index a3v_index1 on t1 using a3v(a vector_l2_ops,b vector_l2_ops,c vector_l2_ops);
explain select * from t1 order by 0.3 * a <-> '[1,1]' + 0.6 * b <-> '[1,1,1]' + 0.4 <-> '[1,1,1,1]' limit 2;
select * from t1 order by 0.3 * a <-> '[1,1]' + 0.6 * b <-> '[1,1,1]' + 0.4 <-> '[1,1,1,1]' limit 2;