from decouple import config
import oracledb
import datetime as dt
import requests
import numpy as np
import logging
import array
import asyncio
from typing import Any, Union, cast

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

url = "http://146.56.142.123:11434/api/embeddings"
headers = {
    'accept': 'application/json',
    'Content-Type': 'application/json',
}
data = {"model" : "bge-m3", "prompt": "What is Oracle AI Vector Search?"}
# 发送 GET 请求
response = requests.post(url, headers=headers, json=data)
if response.status_code == 200:
    # logging.info("成功调用接口:", response.json())
    if isinstance(response.json(), dict):
        record[config("vector_column")] = np.array(response.json()["embedding"]).astype(np.float32)
        result.append(record)
else:
    logging.error(f"请求失败，状态码: {response.status_code}, 错误信息: {response.json()}")