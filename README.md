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

text_embedding_urls： 可以有多个，以逗号分割

query_batch_size：每次从数据库中取的记录数

embedding_batch_size：多少条记录会发送给同一个restapi，因此并发为query_batch_size/embedding_batch_size，如果text_embedding_urls比较少，可能会分到多个需要embedding的batch

vector_column：用来存储向量数据的列，目前从rest api 得到向量数据后强转float32

```
user='<DB_USERNAME>'
password='<DB_PASSWORD>'
dsn='<IP>:1521/<PDB>'
min=1
max=10
increment=1

text_embedding_url='http://<REST API IP>:8899/chat/text_embedding/'

#embedding rest api lists, split with comma
text_embedding_urls='http://<REST API IP>:8899/chat/text_embedding/,http://<REST API IP>:8899/chat/text_embedding/'
#query from DB, maybe the source data is huge, so you can query it by page
query_batch_size=1000
#how many records was sent to one embedding api, so the concurrency is query_batch_size/embedding_batch_size
embedding_batch_size=100
fetch_sql='select id,content from es_docs_10000 order by id offset :1 rows fetch next :2 rows only'
#this column should be primary key or unique
key_column='id'
#this column is origin content which want to be embedded
origin_column='content'
#this column store the VECTOR data
vector_column='vec_content'
update_sql='update es_docs_10000 set vec_content=:1 where id=:2'

```

# 检查embed.py&运行

embed.py脚本中可能有测试的条件信息及输出信息，请自行检查修改。

对于向量后的数据更新，目前脚本也是以批量方式进行更新，可能会存在更新不成功的情况，目前脚本未对更新不成功的数据进行处理。

**运行前请自行保证数据的备份。**

```
python embed.py
```

