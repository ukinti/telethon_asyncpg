Async session for telethon
==========================

Installation
============

::

    pip install telethon_asyncpg
    # Or with poetry
    poetry add telethon_asyncpg


Usage
=====

.. code-block:: python3

    import ssl  # optional

    from telethon import events, TelegramClient

    from telethon_asyncpg import AsyncpgSession, install
    install()

    URI = ???  # URI-string
    # dialect+driver://username:password@host:port/database

    pgconf = dict(dsn=URI, min_size=5, max_size=5)
    # to overcome problem with TLS connection to db pass
    # ssl=ssl.SSLContext(protocol=ssl.PROTOCOL_TLS) to pgconf
    session = AsyncpgSession(pgconf, session_id_factory=???)
    # session_id_factory is any callable with "() -> str" signature
    # default factory is uuid4 str generator. why factory? (it's not really factory ik)
    bot = TelegramClient(session=session, api_id=???, api_hash=???)

    @bot.on(events.NewMessage())
    async def message_handler(message):
        await message.reply("Hi!")

    async def start():
        await bot.start()
        print(await bot.get_entity("martin_winks"))
        await bot.run_until_disconnected()

    if __name__ == '__main__':
        import asyncio
        asyncio.get_event_loop().run_until_complete(start())

- `AsyncpgSession` can also use shared pool by `AsyncpgSession.with_pool` initializer-method

.. code-block:: python

    my_pool = asyncpg.create_pool(...)
    session = AsyncpgSession.with_pool(my_pool, lambda: "session-id", True)


Check out the ``examples/`` folder for more realistic examples.

Contribution
============

Currently we have only asyncpg session available, if you want to contribute with your wrapper - welcome. Take `AsyncpgSession` as an example.

For contributors
================

Patched TelegramClient <-> Session
-----------------------------------

- `TelegramClient` and `Session` object share `settings` `{session.meth: (args_seq, kwargs_mapping)}` dictionary. By protocol `TelegramClient` must add callable with args and kwargs. By protocol session must call this functions at start as it wants (e.g. pass more arguments such as `connection` object to session.method)

- `TelegramClient` may call `session.start` several times per one session instance and session should control its start itself and if it's already started it shouldn't start again

- `Session.save` method is guaranteed to be called as in usual telethon


Hacking
-------

::

    # install poetry dependency manager
    # Fork/Fork+Clone && cd {{cloned}}
    poetry install
    # happy hacking!


References
==========

Telethon: `here <https://github.com/LonamiWebs/telethon>`_
asyncpg pg-driver: `asyncpg <https://github.com/MagicStack/asyncpg>`_
