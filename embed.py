from decouple import config
import oracledb
import datetime
import requests
import numpy as np
import logging
import array
import asyncio
from typing import Any, Union, cast
logger = logging.getLogger("Oracle Database")

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
                        await cursor.execute(sql)
                    else:
                        await cursor.execute(sql, data)
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
async def call_text_embedding(texts: list[dict]):
    url = config("text_embedding_url")
    # 设置请求头
    headers = {
        'accept': 'application/json',
        'Content-Type': 'application/json',
        "access_token": ""
    }
    result = []
    for text in texts:
        try:
            data = {"text": text[config("origin_column")], "embed_model": "bge_m3"}
            # 发送 GET 请求
            response = requests.post(url, headers=headers, json=data)
            # 输出返回的结果
            if response.status_code == 200:
                sss = response.json()
                #logging.info("成功调用接口:", response.json())
                if isinstance(response.json(), dict):
                    if response.json()["code"] == 200:
                        #return response.json()["data"]
                        text[config("vector_column")] = np.array(response.json()["data"].replace("[","").replace("]","").split(",")).astype(np.float64)
                        result.append(text)
            else:
                logging.error(f"请求失败，状态码: {response.status_code}, 错误信息: {response.json()}")
        except Exception as e:
            print(e)
            print(text)
    return result


async def embedding():
    oracle_db = DB()
    query_sql = "select %s,%s from %s " % (config("key_column"), config("origin_column"), config("fetch_table"))
    results: list[dict] = await oracle_db.query(sql=query_sql, data=[])
    #batches = [
    #    results[i: i + int(config("batch_size"))]
    #    for i in range(0, len(results), int(config("batch_size")))
    #]
    #embedding_results =  await asyncio.gather(
    #    *[call_text_embedding(item) for item in batches]
    #)
    columns: list = [config("vector_column"), config("key_column")]
    for item in results:
        datas = []
        embedding_result = await call_text_embedding([item])
        for column in columns:
            datas.append(embedding_result[0][column])
        if len(datas) != 2:
            logging.error("datas length is not suitable")
        else:
            r = await oracle_db.execute(sql=config("update_sql"), data=datas)
            if not r:
                logging.error("update error ", item)

if __name__ == "__main__":
    asyncio.run(embedding())
