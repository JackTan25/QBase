QBase: A Query Aware Vector Search System
------------------------------------------
QBase is developed based on PostgreSQL. It surpass the state of art systems in terms of recall rate 
and p99 latency in the scenario where the query vectors are similar.
## 1.Installion
We can install QBase like Postgres.
```shell
## configure QBase
CFLAGS='-O2 -march=native' ./configure --prefix=/path/to/PGHOME  --enable-debug --with-blocksize=32
## install QBase
make install
## install a3v vector plugin
cd contrib/m3v/a3v/
mkdir build & cd build
cmake -DCMAKE_INSTALL_PREFIX=/path/to/PGHOME -DLIBRARYONLY=ON -DSEEK_ENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Release ..
make install
## init db
initdb -D /path/to/PGDATA
## start pg 
pg_ctl -D /path/to/PGDATA -l logfile start 
```
## 2.Usage of QBase
Multi-Vector Search Usage
```sql
-- multi vector
postgres=# create extension a3v;
postgres=# create table multi_vector_t(emb1 vecetor(4),emb2 vector(4),id int primary key); 
postgres=# insert into multi_vector_t values('[1,1,1,1]','[2,2,2,2]',1),('[3,3,3,3]','[4,4,4,4]',2);
postgres=# insert into multi_vector_t values('[5,5,5,5]','[6,6,6,6]',3),('[7,7,7,7]','[8,8,8,8]',4);
postgres=# insert into multi_vector_t values('[9,9,9,9]','[10,10,10,10]',5),('11,11,11,11]','[12,12,12,12]',6);
postgres=# create index multi_vector_t_a3v_index on t using a3v(emb1 vecotr_l2_ops,emb2 vector_l2_ops);
postgres=# select id from multi_vector_t order by (a <-> '[1,1,1,1]') * w1 + (b <-> '[2,2,2,2]') * w2 limit 1;
-- multi vector + filter
postgres=# select id from multi_vector_t where id < 4 order by (a <-> '[1,1,1,1]') * w1 + (b <-> '[2,2,2,2]') * w2 limit 1;
-- multi vector range
postgres=# select id from multi_vector_t where  (a <-> '[1,1,1,1]') * w1 + (b <-> '[2,2,2,2]') * w2 < 4.5;
```
Single Vector Usage
```sql
-- single vector
postgres=# create extension a3v;
postgres=# create table single_vector_t(emb1 vecetor(4),id int primary key); 
postgres=# insert into single_vector_t values('[1,1,1,1]','[2,2,2,2]',1),('[3,3,3,3]','[4,4,4,4]',2);
postgres=# insert into single_vector_t values('[5,5,5,5]','[6,6,6,6]',3),('[7,7,7,7]','[8,8,8,8]',4);
postgres=# insert into single_vector_t values('[9,9,9,9]','[10,10,10,10]',5),('11,11,11,11]','[12,12,12,12]',6);
postgres=# create index single_vector_t_a3v_index on t using a3v(emb1 vecotr_l2_ops);
postgres=# select id from single_vector_t order by (a <-> '[1,1,1,1]') limit 1;
-- single vector + filter
postgres=# select id from single_vector_t where id < 4 order by (a <-> '[1,1,1,1]') limit 1;
-- single vector range
postgres=# select id from single_vector_t where  (a <-> '[1,1,1,1]') < 2.3;
```
## 3.Contributions
This project welcome contributions. If you have any question, please open an issue on the github platform.