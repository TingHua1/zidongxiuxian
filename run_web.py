import uvicorn

from tg_game.config import get_settings


def _resolve_bind_host(settings) -> str:
    configured_host = str(settings.host or "").strip() or "127.0.0.1"
    if str(settings.domain or "").strip() and configured_host in {
        "127.0.0.1",
        "localhost",
    }:
        return "0.0.0.0"
    return configured_host


def _resolve_access_host(settings, bind_host: str) -> str:
    configured_domain = str(settings.domain or "").strip()
    if configured_domain:
        return configured_domain
    return "127.0.0.1" if bind_host == "0.0.0.0" else bind_host


def _build_uvicorn_kwargs(settings) -> dict:
    certfile = settings.ssl_certfile
    keyfile = settings.ssl_keyfile
    if bool(certfile) != bool(keyfile):
        raise RuntimeError(
            "TG_GAME_SSL_CERTFILE 和 TG_GAME_SSL_KEYFILE 必须同时配置，或同时留空"
        )
    if certfile and not certfile.is_file():
        raise RuntimeError(f"SSL cert file not found: {certfile}")
    if keyfile and not keyfile.is_file():
        raise RuntimeError(f"SSL key file not found: {keyfile}")

    bind_host = _resolve_bind_host(settings)
    kwargs = {
        "host": bind_host,
        "port": settings.port,
        "reload": settings.debug,
    }
    if certfile and keyfile:
        kwargs["ssl_certfile"] = str(certfile)
        kwargs["ssl_keyfile"] = str(keyfile)

    scheme = "https" if certfile and keyfile else "http"
    access_host = _resolve_access_host(settings, bind_host)
    print(f"Web listening on {bind_host}:{settings.port}")
    print(f"Frontend access URL: {scheme}://{access_host}:{settings.port}")
    return kwargs


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "tg_game.web.app:app",
        **_build_uvicorn_kwargs(settings),
    )


if __name__ == "__main__":
    main()
