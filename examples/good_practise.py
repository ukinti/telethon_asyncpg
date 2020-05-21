from telethon import TelegramClient, custom, events
from telethon_asyncpg import install, AsyncpgSession

install()


async def hello_answerer(message: custom.Message):
    await message.reply("https://nohello.com")


async def get_session():
    # do some configuration read etc
    return AsyncpgSession(asyncpg_conf="...postgres_db_dsn...")


async def main():
    session = await get_session()

    client = TelegramClient(
        session=session,
        api_id=1_1_1_1,
        api_hash="change me",
    )

    try:
        await client.start()
        client.add_event_handler(hello_answerer, events.NewMessage())
        await client.run_until_disconnected()
    finally:
        await session.close()


if __name__ == '__main__':
    import asyncio
    asyncio.get_event_loop().run_until_complete(main())
