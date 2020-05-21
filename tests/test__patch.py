import inspect


# todo use pytest.parametrize to check all patched methods
def test_patch_BaseClient_init_annotation(session):
    import telethon

    assert inspect.signature(
        telethon.client.telegrambaseclient.TelegramBaseClient.__init__
    ).parameters.get("session").annotation == 'typing.Union[str, Session]'

    from telethon_asyncpg import install
    install()

    assert inspect.signature(
        telethon.client.telegrambaseclient.TelegramBaseClient.__init__
    ).parameters.get("session").annotation == "AbstractAsyncSession"
