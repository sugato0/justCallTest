import logging
from auto_sender import send_cron
from asyncio import run



# Отключение дочрних логов urllib3
# logging.getLogger("urllib3").propagate = False
logging.getLogger("pyrogram").propagate = False

logging.basicConfig(
    filename='tech.log', #файл логов
    level=logging.INFO, #уровень логов
    format='%(asctime)s - %(levelname)s - %(message)s', #формат логов
    encoding='utf-8' #кодировака логов
)

async def main():
    await send_cron()
run(main())

