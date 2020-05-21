"""
todo(refactor): make more abstract BaseAsyncSession
Session shouldn't know anything about tl/* even session's private interface
"""

from abc import ABC

from ..sessions.abstract import AbstractAsyncSession
from telethon import utils
from telethon.tl import types


class BaseAsyncSession(AbstractAsyncSession, ABC):
    def __init__(self):
        self._dc_id = 0
        self._server_address = None
        self._port = None
        self._auth_key = None
        self._takeout_id = None

        self._files = {}
        self._entities = set()
        self._update_states = {}

    @property
    def dc_id(self):
        return self._dc_id

    @property
    def server_address(self):
        return self._server_address

    @property
    def port(self):
        return self._port

    @property
    def auth_key(self):
        return self._auth_key

    @property
    def takeout_id(self):
        return self._takeout_id

    @classmethod
    def _entity_values_to_row(cls, id, hash, username, phone, name):
        # While this is a simple implementation it might be overrode by,
        # other classes so they don't need to implement the plural form
        # of the method. Don't remove.
        return id, hash, username, phone, name

    def _entity_to_row(self, e):
        if not isinstance(e, types.TLObject):
            return
        try:
            p = utils.get_input_peer(e, allow_self=False)
            marked_id = utils.get_peer_id(p)
        except TypeError:
            # Note: `get_input_peer` already checks for non-zero `access_hash`.
            #        See issues #354 and #392. It also checks that the entity
            #        is not `min`, because its `access_hash` cannot be used
            #        anywhere (since layer 102, there are two access hashes).
            return

        if isinstance(p, (types.InputPeerUser, types.InputPeerChannel)):
            p_hash = p.access_hash
        elif isinstance(p, types.InputPeerChat):
            p_hash = 0
        else:
            return

        username = getattr(e, 'username', None) or None
        if username is not None:
            username = username.lower()
        phone = getattr(e, 'phone', None)
        name = utils.get_display_name(e) or None
        return self._entity_values_to_row(
            marked_id, p_hash, username, phone, name
        )

    def _entities_to_rows(self, tlo):
        if not isinstance(tlo, types.TLObject) and utils.is_list_like(tlo):
            # This may be a list of users already for instance
            entities = tlo
        else:
            entities = []
            if hasattr(tlo, 'user'):
                entities.append(tlo.user)
            if hasattr(tlo, 'chats') and utils.is_list_like(tlo.chats):
                entities.extend(tlo.chats)
            if hasattr(tlo, 'users') and utils.is_list_like(tlo.users):
                entities.extend(tlo.users)

        rows = []  # Rows to add (id, hash, username, phone, name)
        for e in entities:
            row = self._entity_to_row(e)
            if row:
                rows.append(row)
        return rows

