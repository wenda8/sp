import asyncio
from telethon import TelegramClient
from telethon.tl.types import PeerChannel
import pandas as pd
from datetime import datetime
import os
import json

with open('config.json', 'r') as file:
    data = json.load(file)

api_id = data['api_id']
api_hash = data['api_hash']
phone_number = data['phone_number'] 
session_name = data['session_name']

# 要监控的Telegram频道ID
channel_id = 'sportsru'  # 频道ID

# 文件保存路径
output_directory = 'telegram_data'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)

# 定义异步函数来获取消息
async def get_messages(client):
    # 获取频道实体
    channel = await client.get_entity(channel_id)
    messages = await client.get_messages(channel, limit=100)  # 可以调整获取的消息数量

    # 提取消息数据
    data = []
    for message in messages:
        data.append({
            'time': message.date,
            'source': channel_id,
            'text': message.message,
            'media': message.media is not None
        })

    # 转换为DataFrame
    df = pd.DataFrame(data)
    # 保存为Parquet文件
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'{output_directory}/messages_{timestamp}.parquet'
    df.to_parquet(filename, index=False)
    print(f'Saved data to {filename}')

# 定时获取和保存消息
async def periodic_fetch(client, interval_seconds):
    while True:
        await get_messages(client)
        await asyncio.sleep(interval_seconds)

async def main():
    client = TelegramClient(session_name, api_id, api_hash)

    await client.start(phone_number)
    print("Client Created")

    await periodic_fetch(client, interval_seconds=60)  

    await client.disconnect()

loop = asyncio.get_event_loop()
loop.run_until_complete(main())
