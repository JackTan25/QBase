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
安装教程:
// https://blog.csdn.net/Hehuyi_In/article/details/110729822
CFLAGS='-O3 -march=native' ./configure --prefix=/home/tanboyu/cpp_workspace/qbase_data/data
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
