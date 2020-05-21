from .sessions.asyncpg import AsyncpgSession

__telethon_version__ = '1.13.0'
__version__ = "0.1.0"


def install():
    import telethon_asyncpg._patch
