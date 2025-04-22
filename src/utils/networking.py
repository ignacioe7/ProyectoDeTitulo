import asyncio


async def smart_sleep(current_page: int, base_delay: float = 2.0):
    """Pausas adaptativas para evitar bloqueos"""
    if current_page % 100 == 0:
        return await asyncio.sleep(60)
    elif current_page % 50 == 0:
        return await asyncio.sleep(45)
    await asyncio.sleep(base_delay + (current_page // 10))