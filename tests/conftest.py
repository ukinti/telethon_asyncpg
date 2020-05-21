from typing import Dict, Tuple, Any

import pytest

from telethon import TelegramClient
from telethon_asyncpg.sessions import BaseAsyncSession


class Session(BaseAsyncSession):
    async def get_file(self, md5_digest, file_size, cls):
        pass

    async def cache_file(self, md5_digest, file_size, instance):
        pass

    async def get_input_entity(self, key):
        pass

    async def process_entities(self, tlo):
        pass

    async def delete(self):
        pass

    async def save(self):
        pass

    async def close(self):
        pass

    async def set_update_state(self, entity_id, state):
        pass

    async def get_update_state(self, entity_id):
        pass

    async def set_takeout_id(self, takeout_id: int):
        pass

    async def set_auth_key(self, auth_key: str):
        pass

    async def set_dc(self, dc_id, server_address, port):
        pass

    async def start(self, instructions: Dict[str, Tuple[Tuple, Dict[str, Any]]]):
        pass


@pytest.fixture(name="session")
def get_session():
    return Session()


@pytest.fixture()
def client(session: Session):
    return TelegramClient(session, 10, "10")
