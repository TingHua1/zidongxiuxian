import uvicorn

from tg_game.config import get_settings


def main() -> None:
    settings = get_settings()
    uvicorn.run(
        "tg_game.web.app:app",
        host=settings.host,
        port=settings.port,
        reload=settings.debug,
    )


if __name__ == "__main__":
    main()
