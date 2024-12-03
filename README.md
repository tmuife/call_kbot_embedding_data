# 环境信息

- 代码基于python3.12测试通过，请安装python3.12

# 安装第三方包

- pip install -r requirements.txt

# 更改配置信息

```
cp .env.example .env
vim .env
```

参考如下：

从user开始到increment是数据库信息

```
user='vector'
password='xxx#'
dsn='207.211.189.173:1521/vecpdb1'
min=1
max=10
increment=1

text_embedding_url='http://207.211.189.173:8899/chat/text_embedding/'

fetch_table='es_docs_10000'
key_column='id'
origin_column='content'
vector_column='vec_content'
update_sql='update es_docs_10000 set vec_content=:1 where id=:2'

```

# 运行

```
python embed.py
```

