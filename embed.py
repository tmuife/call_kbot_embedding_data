from decouple import config
import oracledb
import datetime as dt
import requests
import numpy as np
import logging
import array
import asyncio
from typing import Any, Union, cast
logger = logging.getLogger("Oracle Database")
logging.basicConfig(format='%(asctime)s - %(pathname)s[line:%(lineno)d] - %(levelname)s: %(message)s',
                    level=logging.INFO)
class DB:

    def __init__(self, **kwargs):
        #############################################
        # if using database installed local, the dsn should be like this:
        # dsn="{}:{}/{}".format(host, port, database)
        # if adw dsn is connection str
        try:
            oracledb.defaults.fetch_lobs = False
            self.pool = oracledb.create_pool_async(
                user=config("user"),
                password=config("password"),
                dsn=config("dsn"),
                min=config("min"),
                max=config("max"),
                increment=config("increment")
            )
            logger.info(f"Connected to Oracle database")
        except Exception as e:
            logger.error(f"Failed to connect to Oracle database")
            logger.error(f"Oracle database error: {e}")
            raise

    def numpy_converter_in(self, value):
        """Convert numpy array to array.array"""
        if value.dtype == np.float64:
            dtype = "d"
        elif value.dtype == np.float32:
            dtype = "f"
        else:
            dtype = "b"
        return array.array(dtype, value)

    def input_type_handler(self, cursor, value, arraysize):
        """Set the type handler for the input data"""
        if isinstance(value, np.ndarray):
            return cursor.var(
                oracledb.DB_TYPE_VECTOR,
                arraysize=arraysize,
                inconverter=self.numpy_converter_in,
            )
    def numpy_converter_out(self, value):
        """Convert array.array to numpy array"""
        if value.typecode == "b":
            dtype = np.int8
        elif value.typecode == "f":
            dtype = np.float32
        else:
            dtype = np.float64
        return np.array(value, copy=False, dtype=dtype)

    def output_type_handler(self, cursor, metadata):
        """Set the type handler for the output data"""
        if metadata.type_code is oracledb.DB_TYPE_VECTOR:
            return cursor.var(
                metadata.type_code,
                arraysize=cursor.arraysize,
                outconverter=self.numpy_converter_out,
            )

    async def execute(self, sql: str, data: list = None) -> Union[bool, None]:
        # logger.info("go into OracleDB execute method")
        try:
            async with self.pool.acquire() as connection:
                connection.inputtypehandler = self.input_type_handler
                connection.outputtypehandler = self.output_type_handler
                with connection.cursor() as cursor:
                    if data is None:
                        await cursor.executemany(sql)
                    else:
                        await cursor.executemany(sql, data)
                    await connection.commit()
                return True
        except Exception as e:
            logger.error(f"Oracle database error: {e}")
            logger.error(sql)
            logger.error(data)
            raise

    async def query(self,sql: str, data: list = None) -> list[dict]:
       async with self.pool.acquire() as connection:
            connection.inputtypehandler = self.input_type_handler
            connection.outputtypehandler = self.output_type_handler
            with connection.cursor() as cursor:
                try:
                    if data:
                        await cursor.execute(sql, data)
                    else:
                        await cursor.execute(sql)
                except Exception as e:
                    logger.error(f"Oracle database error: {e}")
                    logger.error(sql)
                    raise
                columns = [column[0].lower() for column in cursor.description]
                rows = await cursor.fetchall()
                if rows:
                    return [dict(zip(columns, row)) for row in rows]
                else:
                    return []
async def call_text_embedding(data_batch: dict):
    url = data_batch.get("api", config("text_embedding_url"))
    # 设置请求头
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        "access_token": ""
    }
    result = []
    records = data_batch["datas"]
    for record in records:
        try:
            data = {"text": record[config("origin_column")], "embed_model": "bge_m3"}
            # 发送 GET 请求
            response = requests.post(url, headers=headers, json=data)
            # 输出返回的结果
            if response.status_code == 200:
                #logging.info("成功调用接口:", response.json())
                if isinstance(response.json(), dict):
                    if response.json()["code"] == 200:
                        record[config("vector_column")] = np.array(response.json()["data"].replace("[","").replace("]","").split(",")).astype(np.float32)
                        result.append(record)
            else:
                logging.error(f"请求失败，状态码: {response.status_code}, 错误信息: {response.json()}")
        except Exception as e:
            logging.error(e)
            logging.error(record)
    return result


async def embedding():
    oracle_db = DB()
    query_sql = config("fetch_sql")
    update_sql = config("update_sql")
    query_batch_size = int(config("query_batch_size"))
    embedding_batch_size = int(config("embedding_batch_size"))
    rest_api_arr = config("text_embedding_urls").split(",")
    index = 0
    while True:
        start = index*query_batch_size
        end = start+query_batch_size
        query_result = await oracle_db.query(sql=query_sql, data=[start, end])
        logging.info("The %sth query batch was query out and start to call embedding concurrently at %s" % (index+1, dt.datetime.now()))
        api_with_data_object_list = []
        batches = [
           query_result[i: i + embedding_batch_size]
           for i in range(0, len(query_result), embedding_batch_size)
        ]
        for i, batch in enumerate(batches):
            assigned_rest_api = rest_api_arr[i % len(rest_api_arr)]  # 根据索引进行轮询分配
            api_with_data_object_list.append({"api": assigned_rest_api, "datas": batch})

        embedding_results = await asyncio.gather(
           *[call_text_embedding(item) for item in api_with_data_object_list]
        )
        logging.info("The %sth query batch embedding finish at %s" % (index + 1, dt.datetime.now()))

        data_params = []
        for _embeds in embedding_results:
            for _embed in _embeds:
                data_params.append([_embed[config("vector_column")], _embed[config("key_column")]])
        insert_result = await oracle_db.execute(sql=update_sql, data=data_params)
        if not insert_result:
            logging.error("update error batch data", embedding_results)
        logging.info("The %sth query batch updated finish at %s" % (index + 1, dt.datetime.now()))
        index += 1
        if len(query_result) < query_batch_size:
            break


if __name__ == "__main__":
    asyncio.run(embedding())
