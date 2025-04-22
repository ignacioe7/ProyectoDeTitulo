import asyncio
from src.ui.cli import ScrapeAllCLI
from src.utils.logger import setup_logging

def main():
    setup_logging()
    cli = ScrapeAllCLI()
    asyncio.run(cli.run())

if __name__ == "__main__":
    main()