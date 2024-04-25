Depencies installation:
```shell
sudo yum -y install readline-devel
sudo yum -y install python-devel
sudo yum -y install perl-devel
sudo yum -y install libuuid-devel
sudo yum -y install gcc
sudo yum -y install flex
sudo yum -y install bison
sudo yum -y install perl-ExtUtils-Embed
sudo yum -y install zlib-devel   
sudo yum -y install systemd-devel.i686
sudo yum -y install systemd-devel.x86_64
```
一个坑: 这里的qbase_data不要手动创建,直接configure然后make;make install; 然后initdb -D /home/tanboyu/cpp_workspace/qbase_data/data
让postgres自己创建,否则出现权限问题很麻烦
安装教程:
// https://blog.csdn.net/Hehuyi_In/article/details/110729822
CFLAGS='-O3 -march=native' ./configure --prefix=/home/tanboyu/cpp_workspace/qbase_data/
// debug
CFLAGS='-O3 -march=native' ./configure --prefix=/home/tanboyu/cpp_workspace/qbase_data/  --enable-debug

./configure --prefix=/home/tanboyu/cpp_workspace/qbase_data/ --with-libedit-preferred --with-perl --with-python --with-uuid=e2fs --with-systemd --enable-debug --enable-dtrace CFLAGS="-g -O0"

配置postgres库用于find_package
```shell
## pg_config --includedir-server
cmake -DCMAKE_INSTALL_PREFIX=/home/tanboyu/cpp_workspace/qbase_data/data -DLIBRARYONLY=ON -DSEEK_ENABLE_TESTS=ON -DCMAKE_BUILD_TYPE=Release ..
```
格式转换:
```shell
awk '{
    printf "[";
    for (i = 1; i <= NF; i++) {
        printf "%s", $i;
        if (i < NF) {
            printf ",";
        }
    }
    printf "]\n";
}' bigann_vector_128_1M.txt > bigann_vector_128_postgres_1M.txt
1 2 3 --> [1,2,3]

sed 's/ /,/g' bigann_vector_128_1M.txt > bigann_vector_128_vbase_1M.txt
1 2 3 --> 1,2,3

awk '{
    printf "{";
    for (i = 1; i <= NF; i++) {
        printf "%s", $i;
        if (i < NF) {
            printf ",";
        }
    }
    printf "}\n";
}' bigann_vector_128_1M.txt > bigann_vector_128_vbase_1M.txt
1 2 3 --> {1,2,3}
```

sudo systemctl daemon-reload
sudo systemctl restart docker

pg_ctl -D /home/tanboyu/cpp_workspace/qbase_data/data/ -l logfile stop
pg_ctl -D /home/tanboyu/cpp_workspace/qbase_data/data/ -l logfile start

multi_bitmap_sacn: do prefilter:
```sql
CREATE TABLE my_table (
  id SERIAL PRIMARY KEY,
  int_field INT,
  text_field TEXT
);

CREATE INDEX idx_int_field2 ON my_table (int_field);
CREATE INDEX idx_text_field2 ON my_table (text_field);

INSERT INTO my_table (int_field, text_field)
SELECT (random() * 1000000)::int AS random_int,
       md5(random()::text) AS random_text
FROM generate_series(1, 1000000);
```
```sql
create extension m3v;
create table test_prefilter(a int,b text,c vector(5));
\copy test_prefilter from '/home/tanboyu/cpp_workspace/qbase_experiments/test_prefilter.csv' WITH (FORMAT csv, DELIMITER '|');
CREATE INDEX idx_int_field ON test_prefilter (a);
CREATE INDEX idx_text_field ON test_prefilter (b);
set enable_seqscan = off;
\timing on
select pg_backend_pid();
select * from test_prefilter where a < 90 and c <-> '[5,17,28,19,37]' < 87;
```

src/backend/tcop/postgres.c
```c
/* call the optimizer */
// optimize plan for:
// select * from t where attr_condoition order by vector_col <-> xxx limit k;
// transform into multi bitmap index scan, and attr index first then push down 
// it into vector index.
plan = planner(querytree, query_string, cursorOptions, boundParams);
```


// src/backend/optimizer/plan/planner.c
```c
// 在这里可以拿到upper_rel的cheapest_total_path.
// 他是limit path, index_scan_path啥的,也是构成一棵树在后面就根据这个
// 树构建查询节点去安装火山模型去执行的.
grouping_planner(root, tuple_fraction);
```

// src/backend/optimizer/plan/planner.c
```c
/*
    * Generate the best unsorted and presorted paths for the scan/join
    * portion of this Query, ie the processing represented by the
    * FROM/WHERE clauses.  (Note there may not be any presorted paths.)
    * We also generate (in standard_qp_callback) pathkey representations
    * of the query's sort clause, distinct clause, etc.
    */
current_rel = query_planner(root, standard_qp_callback, &qp_extra);
```

// src/backend/optimizer/path/allpaths.c
```c
static void
set_plain_rel_pathlist(PlannerInfo *root, RelOptInfo *rel, RangeTblEntry *rte)
```

// src/backend/optimizer/path/indxpath.c
```c
// 设置bit mapindexs scan
void create_index_paths(PlannerInfo *root, RelOptInfo *rel)

```

最后我发现:
explain (verbose) select * from my_table where id < 2000 and  int_field > 999990 limit 3;
经过优化器的操作后,最后得到的路径结构是:
LimitPath
    |
BitmapHeapPath
    |
IndexPath
然后依据此去构建查询计划,在下面这里它把它转换为了一个bitmap_index_scan
src/backend/optimizer/plan/createplan.c
```c
/* Use the regular indexscan plan build machinery... */
iscan = castNode(IndexScan,
                    create_indexscan_plan(root, ipath,
                                        NIL, NIL, false));
/* then convert to a bitmap indexscan */
plan = (Plan *)make_bitmap_indexscan(iscan->scan.scanrelid,
                                        iscan->indexid,
                                        iscan->indexqual,
                                        iscan->indexqualorig);
```
我们的目标是改进:
select * from my_table where id < 2000 order by vector <-> '[...]' limit k;
但是他生成的计划是:
IndexScan
    Filter (id < 2000)
我们要改成

// src/backend/optimizer/path/indxpath.c
```c
get_index_paths()里面修改存在vector index scan的情况:
	foreach (lc, indexpaths)
	{
		IndexPath *ipath = (IndexPath *)lfirst(lc);

		if (index->amhasgettuple)
			add_path(rel, (Path *)ipath);
		float old_selectivity = ipath->indexselectivity;
        // 在考虑一下设置path.start_up_cost和path.total_cost保证它排序在其它索引扫描完之后
		if(ipath->is_vector_search) ipath->indexselectivity = 0.0;
		if (index->amhasgetbitmap &&
			(ipath->path.pathkeys == NIL ||
			 ipath->indexselectivity < 1.0))
			*bitindexpaths = lappend(*bitindexpaths, ipath);
		ipath->indexselectivity = old_selectivity;
	}
然后在create_index_paths当中:
	if (bitindexpaths != NIL)
	{
		Path *bitmapqual;
		BitmapHeapPath *bpath;
        // 主要是这个函数
		bitmapqual = choose_bitmap_and(root, rel, bitindexpaths);
		bpath = create_bitmap_heap_path(root, rel, bitmapqual,
										rel->lateral_relids, 1.0, 0);
		add_path(rel, (Path *)bpath);

		/* create a partial bitmap heap path */
		if (rel->consider_parallel && rel->lateral_relids == NULL)
			create_partial_bitmap_paths(root, rel, bitmapqual);
	}

static Path *
choose_bitmap_and(PlannerInfo *root, RelOptInfo *rel, List *paths)
在这里面检测拍完序后在构造bitmapand返回.
	qsort(pathinfoarray, npaths, sizeof(PathClauseUsage *),
		  path_usage_comparator);
// 目的是先其它索引执行完再来执行索引扫描,这样查询计划就修正完了,对于查询执行部分则是需要在MultiBitmapAndIndexScan执行阶段
// 将位图下推下去.
```


