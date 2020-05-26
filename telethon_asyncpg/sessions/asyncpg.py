"""
schema for asyncpg sessions is "asyncpg_telethon"
"""

from typing import List, Optional, Dict, Any, Union, Callable, Tuple
import asyncio
import uuid
from abc import ABC
import datetime
import logging

import asyncpg

from telethon import utils
from telethon.crypto import AuthKey
from telethon.tl import types
from ..sessions.base import BaseAsyncSession

TABLES = ("sessions", "sent_files", "update_state",)
TELETHON_SQLITE_CURRENT_VERSION = 6  # database versions must be the same as telethon's original SQLite version
ALLOWED_ENTITY_IDENTIFIER_NAMES = ("name", "username", "phone", )

logger = logging.getLogger(__name__)

_sfconf = {
    types.InputDocument: 0,
    types.InputPhoto: 1,
}

_sfconf_keys = tuple(_sfconf.keys())


def default_session_id_factory() -> str:
    return str(uuid.uuid4())


def _sftype(cls):
    if cls not in _sfconf:
        raise ValueError('The cls must be either InputDocument/InputPhoto')
    return _sfconf[cls]


async def check_tables(connection: asyncpg.Connection) -> bool:
    for table in TABLES:
        rec = await connection.fetchval(
            """
            select EXISTS(
            select 1
            from pg_tables
            where schemaname = $1 and tablename = $2);
            """, "asyncpg_telethon", table
        )

        if bool(rec) is False:
            logger.debug(f"Table = {table} with schema `asyncpg_telethon` does not exist")
            return False

    return True


async def create_tables(connection: asyncpg.Connection, lock: asyncio.Lock):
    create_tables_sql = (
        """sessions (
            session_id varchar(255),
            dc_id integer,
            server_address text,
            port integer,
            auth_key bytea,
            takeout_id integer,
            primary key(session_id, dc_id)
        )""",
        """entities (
            session_id varchar(255),
            id bigint,
            hash bigint not null,
            username text ,
            phone bigint default null,
            name text,
            primary key(session_id, id)
        )""",
        """sent_files (
            session_id varchar(255),
            md5_digest bytea,
            file_size integer,
            type integer,
            id bigint,
            hash bigint,
            primary key(session_id, md5_digest, file_size, type)
        )""",
        """update_state (
            session_id varchar(255),
            id integer,
            pts integer,
            qts integer,
            date integer,
            seq integer,
            primary key(session_id, id)
        )""")

    async with lock:
        logger.debug(f"Creating schema(`asyncpg_telethon`) and tables: {TABLES} with schema `asyncpg_telethon`")
        async with connection.transaction(isolation="read_committed"):
            await connection.execute("""create schema if not exists "asyncpg_telethon";""")
            # table is sure safe to be passed by f'' to query.
            await connection.execute("".join(
                f"""create table if not exists "asyncpg_telethon".{table};"""
                for table in create_tables_sql
            ))
        logger.debug("Tables created")


class AsyncpgSession(BaseAsyncSession, ABC):
    def __init__(
        self,
        asyncpg_conf: Optional[Union[str, Dict[str, Any]]],
        session_id_factory: Callable[[], str] = default_session_id_factory,
        save_entities: bool = True
    ):
        """
        Initializer for AsyncpgSession
        :param asyncpg_conf: asyncpg.create_pool configs
        :param session_id_factory: function to generate session identifier
        :param save_entities: True - all entities will be cached while processing
        """
        super().__init__()
        self._session_id = session_id_factory()
        self.save_entities = save_entities
        self._conf = asyncpg_conf if isinstance(asyncpg_conf, dict) else {"dsn": asyncpg_conf}
        self._pool: Optional[asyncpg.pool.Pool] = None
        self._lock = asyncio.Lock()
        self._conn: Optional[asyncpg.Connection] = None

    @classmethod
    def with_pool(
        cls,
        asyncpg_pool: asyncpg.pool.Pool,
        session_id_factory: Callable[[], str] = default_session_id_factory,
        save_entities: bool = True
    ) -> 'AsyncpgSession':
        """
        Another implementation of initializer but for shared pool
        :param asyncpg_pool: ready asyncpg Pool
        :param session_id_factory: ...
        :param save_entities: ...
        :return: instance of AsyncpgSession
        """
        self = cls(
            asyncpg_conf=None,
            session_id_factory=session_id_factory,
            save_entities=save_entities
        )  # type: ignore
        self._pool = asyncpg_pool
        return self

    async def start(self, settings: Dict[Callable[..., Any], Tuple[Tuple[Any, ...], Dict[str, Any]]]):
        if self.started:
            return

        if not isinstance(self._pool, asyncpg.pool.Pool):
            # no more than one pool should be opened in one instance
            self._pool = await asyncpg.create_pool(**self._conf)

        async with self._pool.acquire() as conn:  # type: asyncpg.Connection
            if (await check_tables(conn)) is False:
                await create_tables(conn, self._lock)

            async with self._lock:
                for method, (args, kwargs) in settings.items():
                    try:
                        self._conn = conn
                        await method(*args, **kwargs)
                    except asyncpg.InterfaceError as exc:
                        await self.close()
                        raise exc
                else:
                    self.started = True
                    self._conn = None
                    await conn.close()

    async def set_dc(self, dc_id, server_address, port):
        # Fetch the auth_key corresponding to this data center
        self._dc_id = dc_id
        self._port = port
        self._server_address = server_address
        await self._update_session_table()

    async def set_auth_key(self, auth_key: AuthKey):
        self._auth_key = AuthKey(data=auth_key)
        await self._update_session_table()

    async def set_takeout_id(self, takeout_id: int):
        self._takeout_id = takeout_id
        await self._update_session_table()

    async def _update_session_table(self):
        """
        Method is not responsible to close connection he hasn't opened.
        """
        query, args = """
            insert into 
            asyncpg_telethon.sessions (session_id, dc_id, server_address, port, auth_key, takeout_id) 
            values ($1, $2, $3, $4, $5, $6) 
            on conflict(session_id, dc_id) do 
            update set dc_id = $2,
            server_address = $3, port = $4,
            auth_key = $5, takeout_id = $6 
            where sessions.session_id = $1;
        """, (
            self._session_id,
            self._dc_id,
            self._server_address,
            self._port,
            self._auth_key.key if self._auth_key else b'',
            self._takeout_id
        )

        if self._conn is not None:
            await self._conn.execute(query, *args)
            return

        else:
            async with self._pool.acquire() as conn:  # type: asyncpg.Connection
                await conn.execute(query, *args)

    async def get_update_state(self, entity_id):
        query = """
            select pts, qts, date, seq
            from asyncpg_telethon.update_state 
            where update_state.session_id = $1 and id = $2;
        """

        async with self._pool.acquire() as conn:  # type: asyncpg.Connection
            row = await conn.fetchrow(
                query, self._session_id, entity_id
            )
            if row:
                pts, qts, date, seq = row.values()
                date = datetime.datetime.fromtimestamp(date, tz=datetime.timezone.utc)
                return types.updates.State(pts, qts, date, seq, unread_count=0)

    async def set_update_state(self, entity_id, state, connection: asyncpg.Connection = None):
        query = """
            insert into asyncpg_telethon.update_state(session_id, id, pts, qts, date, seq) values ($2,$3,$4,$5,$6,$7) 
            on conflict(session_id, id) do 
            update set id = $2, pts = $3, qts = $4, date = $5, seq = $6 
            where update_state.session_id = $1;
        """
        async with self._pool.acquire() as conn:  # type: asyncpg.Connection
            await conn.execute(
                query, self._session_id, entity_id, state.pts,
                state.qts, state.date.timestamp(), state.seq
            )

    async def process_entities(self, tlo):
        """Processes all the found entities on the given TLObject,
           unless .enabled is False.

           Returns True if new input entities were added.
        """
        if not self.save_entities:
            return

        rows = self._entities_to_rows(tlo)
        if not rows:
            return

        query = """
            insert into asyncpg_telethon.entities(session_id, id, hash, username, phone, name) 
            values ($1,$2,$3,$4,$5,$6) 
            on conflict(session_id, id) do 
            update set id = $2, hash = $3, username = $4, phone = $5, name = $6 
            where entities.session_id = $1;
        """

        for n, row in enumerate(rows):
            row = list(row)
            row.insert(0, self._session_id)
            rows[n] = row

        async with self._pool.acquire() as conn:  # type: asyncpg.Connection
            await conn.executemany(query, rows)

    async def _get_entities_by_x(self, coln: str, colval: str) -> List[asyncpg.Record]:
        """
        _get_entities_by_x should never been called from outside.
        """
        if coln not in ALLOWED_ENTITY_IDENTIFIER_NAMES:
            raise RuntimeWarning(f"{coln!s} is not a valid tablename for entity")

        query = """
            select id, hash from asyncpg_telethon.entities
            where 
            entities.session_id = $1 and $2 = $3;
        """

        async with self._pool.acquire() as conn:  # type: asyncpg.Connection
            return await conn.fetch(query, self._session_id, coln, colval,)

    async def get_entity_rows_by_phone(self, phone):
        return await self._get_entities_by_x("phone", phone)

    async def get_entity_rows_by_username(self, username):
        return await self._get_entities_by_x("username", username)

    async def get_entity_rows_by_name(self, name):
        return await self._get_entities_by_x("name", name)

    async def get_entity_rows_by_id(self, id, exact=True):
        async with self._pool.acquire() as conn:  # type: asyncpg.Connection
            if exact:
                return await conn.fetch(
                    """
                    select id, hash from asyncpg_telethon.entities
                    where
                    entities.session_id = $1 and id = $2
                    """,
                    self._session_id,
                    id
                )

            return await conn.fetch(
                """
                select id, hash from asyncpg_telethon.entities
                where
                entities.session_id = $1 and id in ($2,$3,$4)
                """,
                self._session_id,
                utils.get_peer_id(types.PeerUser(id)),
                utils.get_peer_id(types.PeerChat(id)),
                utils.get_peer_id(types.PeerChannel(id))
            )

    # File processing

    async def get_file(self, md5_digest, file_size, cls):
        query = """
            select id, hash from asyncpg_telethon.sent_files 
            where
                sent_files.session_id = $1 
                and 
                md5_digest = $2
                and
                file_size = $3
                and
                type = $4;
        """
        async with self._pool.acquire() as conn:  # type: asyncpg.Connection
            row = await conn.fetchrow(
                query,
                self._session_id,
                md5_digest, file_size, _sftype(cls)
            )
            if row:
                # Both allowed classes have (id, access_hash) as parameters
                return cls(row.get("id"), row.get("hash"))

    async def cache_file(self, md5_digest, file_size, instance):
        if not isinstance(instance, _sfconf_keys):
            raise TypeError('Cannot cache %s instance' % type(instance))

        query = """
            insert into asyncpg_telethon.sent_files(session_id,md5_digest,file_size,type,id,hash) 
            values ($1,$2,$3,$4,$5,$6) 
            on conflict(session_id, md5_digest, file_size, type) do 
            update set md5_digest = $2, file_size = $3, type = $4, id = $5, hash = $6 
            where sent_files.session_id = $1;
        """

        async with self._pool.acquire() as conn:  # type: asyncpg.Connection
            await conn.execute(
                query,
                self._session_id,
                md5_digest, file_size,
                _sftype(type(instance)),
                instance.id,
                instance.access_hash
            )

    async def delete(self):
        async with self._pool.acquire() as conn:
            async with conn.transaction():
                for table in TABLES:
                    await conn.execute(
                        """
                        delete from asyncpg_telethon.$1
                        where
                        $1.session_id = $2;
                        """,
                        table, self._session_id
                    )

    async def get_input_entity(self, key):
        try:
            if key.SUBCLASS_OF_ID in (0xc91c90b6, 0xe669bf46, 0x40f202fd):
                # hex(crc32(b'InputPeer', b'InputUser' and b'InputChannel'))
                # We already have an Input version, so nothing else required
                return key
            # Try to early return if this key can be casted as input peer
            return utils.get_input_peer(key)
        except (AttributeError, TypeError):
            # Not a TLObject or can't be cast into InputPeer
            if isinstance(key, types.TLObject):
                key = utils.get_peer_id(key)
                exact = True
            else:
                exact = not isinstance(key, int) or key < 0

        result = None
        if isinstance(key, str):
            phone = utils.parse_phone(key)
            if phone:
                result = await self.get_entity_rows_by_phone(phone)
            else:
                username, invite = utils.parse_username(key)
                if username and not invite:
                    result = await self.get_entity_rows_by_username(username)
                else:
                    tup = utils.resolve_invite_link(key)[1]
                    if tup:
                        result = await self.get_entity_rows_by_id(tup, exact=False)

        elif isinstance(key, int):
            result = await self.get_entity_rows_by_id(key, exact)

        if not result and isinstance(key, str):
            result = await self.get_entity_rows_by_name(key)

        if result:
            entity_id, entity_hash = result  # unpack resulting tuple
            entity_id, kind = utils.resolve_id(entity_id)
            # removes the mark and returns type of entity
            if kind == types.PeerUser:
                return types.InputPeerUser(entity_id, entity_hash)
            elif kind == types.PeerChat:
                return types.InputPeerChat(entity_id)
            elif kind == types.PeerChannel:
                return types.InputPeerChannel(entity_id, entity_hash)
        else:
            raise ValueError('Could not find input entity with key ', key)

    async def close(self, timeout: int = None):
        """
        Implements connection pool closing.

        :param timeout: Ignored in this implementation
        """
        await self._pool.close()
        self.started = False

    async def save(self):
        """
        AsyncpgSession does not define save metthod.
        """
