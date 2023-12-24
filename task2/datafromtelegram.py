import asyncio
from telethon import TelegramClient
from telethon.tl.types import PeerChannel
import pandas as pd
from datetime import datetime
import os

api_id = '22517422'    
api_hash = '917a6cc7c2180bb526f1cb701b436e95'  
phone_number = '+79819129854'  
session_name = '+79819129854.session'  
channel_id = 'sportsru' 

output_directory = 'telegram_data'
if not os.path.exists(output_directory):
    os.makedirs(output_directory)


async def get_messages(client):
    channel = await client.get_entity(channel_id)
    messages = await client.get_messages(channel, limit=100) 

    data = []
    for message in messages:
        data.append({
            'time': message.date,
            'source': channel_id,
            'text': message.message,
            'media': message.media is not None
        })


    df = pd.DataFrame(data)

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f'{output_directory}/messages_{timestamp}.parquet'
    df.to_parquet(filename, index=False)
    print(f'Saved data to {filename}')


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
