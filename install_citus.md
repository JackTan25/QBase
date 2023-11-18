在配置安装citus的过程当中,我们会遇到很多问题,总结下步骤如下:
```text
1. git clone 
2. 我们需要按照https://github.com/citusdata/citus/blob/main/CONTRIBUTING.md把依赖装一下,
但是postgres14这个相关的就不用了
3. 确保我们已经安装了postgres在本地
4. 在citus下面执行./configure 会在src/include下生成了很多文件,将citus的文件全部放到postgres的src/include下面去,
在/data/data/postgresql.conf 里面添加shared_preload_libraries='citus'
5. 在citus下make和make install 
6.按照官网继续执行
pip install pipenv
pipenv --rm
// 这里会遇到    from .vendor.pip_shims.shims import InstallCommand ImportError: cannot import name 'InstallCommand'
// 参考 https://www.caspertsui.blog/pipenv/ 执行 pip3 install pipenv==2022.3.23
pipenv install 
pipenv shell
```

echo "shared_preload_libraries = 'citus'" >> /data/data/postgresql.conf

./configure --prefix=/data --with-libedit-preferred --with-perl --with-python --with-uuid=e2fs --with-systemd --enable-debug --enable-dtrace CFLAGS="-g -O0" --without-icu