"""
---
mod stands for module not modulo :)
"""

import inspect
import importlib
from typing import TypeVar, Tuple, cast

AMod = TypeVar("AMod")
BMod = TypeVar("BMod")

IS_PATCHED = False


def do_import(
    a_mod: str,
    klass: str,
    b_mod_replace: Tuple[str, str]
) -> Tuple[AMod, BMod]:
    a_lib_impl = importlib.import_module(a_mod)
    b_lib_impl = importlib.import_module(a_mod.replace(*b_mod_replace))

    return (
        cast(AMod, getattr(a_lib_impl, klass)),
        cast(BMod, getattr(b_lib_impl, klass))
    )


def patch(a_type: AMod, b_type: BMod, *include: str) -> None:
    for method in include:
        if method == "*":
            for name, b_obj in inspect.getmembers(b_type):
                setattr(a_type, name, b_obj)

            return
        setattr(a_type, method, getattr(b_type, method))


try:
    import telethon

    B_REPLACE = ("telethon", "telethon_asyncpg")

    A, B = do_import("telethon.client.auth", "AuthMethods", B_REPLACE)
    patch(A, B, "_start", "start")

    A, B = do_import("telethon.client.downloads", "_DirectDownloadIter", B_REPLACE)
    patch(A, B, "_init")

    A, B = do_import("telethon.client.telegrambaseclient", "TelegramBaseClient", B_REPLACE)
    patch(A, B, "__init__", "connect", "_disconnect", "_switch_dc", "_auth_key_callback")

    A, B = do_import("telethon.client.updates", "UpdateMethods", B_REPLACE)
    patch(A, B, "_handle_update", "_update_loop", "_dispatch_update")

    A, B = do_import("telethon.client.users", "UserMethods", B_REPLACE)
    patch(A, B, "_call", "get_input_entity", )

    A, B = do_import("telethon.network.mtprotosender", "MTProtoSender", B_REPLACE)
    patch(A, B, "_try_gen_auth_key", )

    IS_PATCHED = True
except ImportError as exc:
    raise RuntimeError("Remember: `patch` will not work without telethon") from exc


__all__ = []
