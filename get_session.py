import asyncio
from telethon.sync import TelegramClient
from telethon.sessions import StringSession
from dotenv import load_dotenv
import os

async def main():
    load_dotenv('.env')
    api_id = int(os.getenv('API_ID'))
    api_hash = os.getenv('API_HASH')
    
    async with TelegramClient('tg_user.session', api_id, api_hash) as client:
        session_string = StringSession.save(client.session)
        print('=' * 50)
        print('ВАШ STRING_SESSION:')
        print(session_string)
        print('=' * 50)
        print('Скопируйте всю строку выше - она понадобится для настройки сервера!')

if __name__ == "__main__":
    asyncio.run(main())