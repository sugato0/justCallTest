from sqlite3 import OperationalError
from db import messages_db,accounts_db
import logging
from pyrogram import Client
from asyncio import sleep,create_task
from datetime import datetime
from pyrogram.types import InputPhoneContact
from pyrogram.errors.exceptions.bad_request_400 import PeerIdInvalid
from pyrogram.errors import PeerIdInvalid, FloodWait, RPCError,ApiIdInvalid,ChatIdInvalid,ContactIdInvalid,ContactAddMissing
from dotenv import load_dotenv
from os import getenv
load_dotenv()

FROM_ENV = True

async def send(from_account: str, to_account: str, text: str, error_index:int = 0):
    """
    Асинхронная функция для отправки сообщения от одного аккаунта другому.

    Параметры
    ---------
        from_account (str): Номер телефона аккаунта, от которого отправляется сообщение.
        to_account (str): Номер телефона аккаунта, которому отправляется сообщение.
        text (str): Текст сообщения для отправки.
        error_index (int): Проверка на наличие ошибки в предыдущем цикле для избежания бесконечных циклов (до 5 раз)
    Возвращаемое значение
    ---------------------
        dict: Словарь с результатом выполнения операции, содержащий:
            - status_code (int): Код статуса операции (1 - успешно, 2 - ошибка).
            - error_text (str): Текст ошибки, если она произошла, иначе None.

    Исключения
    ----------
        ValueError: Если произошла ошибка в значениях при подключениях/другихоперциях в коде.
        ApiIdInvalid: Если API ID недействителен.
        ConnectionError: Если произошла ошибка подключения.
        Exception: Если произошла любая другая ошибка.
        PeerIdInvalid: Пользователь не найден
        RPCError: проблемы с параметрами запроса
        ContactAddMissing: конфликты контактов/запроса
        FloodWait: Ожидание и рекурсия на повторную отправку
        ChatIdInvalid: Не найден чат, или не существует
    """
    # Формируем имя сессии на основе идентификатора аккаунта
    session_name = f"sessions/{from_account}"
    #Вариант получения аккаунтов из accounts.csv
    
    if FROM_ENV:
        api_id = str(getenv(f"API_ID_{from_account}"))
        api_hash = str(getenv(f"API_HASH_{from_account}"))
    else:
        account = accounts_db.get_lines(
            key = "phone",
            value = from_account
        )
        api_id = account.get("api_id")
        api_hash = account.get("api_hash")


    try:
        # Подключаемся к аккаунту с использованием клиента
        async with Client(name = session_name,api_id=api_id,api_hash=api_hash,phone_number=from_account) as app:
            try:
                # Пытаемся отправить сообщение
                await app.send_message(
                    chat_id=to_account, 
                    text=text,
                )
                # Логируем успешную отправку сообщения
                logging.info(f"Отправлено сообщение с текстом {text} от: {from_account} кому: {to_account} ")
                return {
                    "status_code": 1,
                    "error_text": None
                }
                
            except PeerIdInvalid:
                # Если получатель не найден, пытаемся добавить его в контакты
                logging.info(f"Получатель по {to_account} не найден")
                try:
                    await app.import_contacts([
                        InputPhoneContact(
                            phone=to_account, 
                            first_name=to_account
                        )
                    ])
                    # Логируем успешное добавление в контакты
                    logging.info(f"Получатель по {to_account} успешно добавлен в контакты")
                
                except ContactAddMissing as e:
                    # Логируем ошибку при добавлении в контакты
                    logging.error(f"ContactAddMissing: Невозможно добавить получателя по {to_account} в контакты: {e}")
                    return {
                        "status_code": 2,
                        "error_text": f"ContactAddMissing: Could not add a recipient to contacts"
                    }
                
                # Повторно пытаемся отправить сообщение после добавления в контакты
                return await send(from_account, to_account, text,error_index+1)
            
            except FloodWait as e:
                # Если требуется ожидание из-за ограничений, ждем и повторяем отправку
                logging.warning(f"Необходимо подождать {e.x} секунд перед отправкой сообщения")
                await sleep(e.x)
                return await send(from_account, to_account, text,error_index+1)
            
            except RPCError as e:
                # Логируем ошибку при отправке сообщения
                logging.error(f"Ошибка при отправке сообщения: {e}")
                return {
                    "status_code": 2,
                    "error_text": str(e)
                }
            
            except ChatIdInvalid as e:
                # Логируем ошибку при отправке сообщения
                logging.error(f"Ошибка при отправке сообщения: {e}")
                return {
                    "status_code": 2,
                    "error_text": str(e)
                }
            
    except ValueError as e:
        # Логируем ошибку при подключении к аккаунту
        logging.error(f"ValueError: Ошибка при подключении к аккаунту {from_account}: {e}")
        return {
            "status_code": 2,
            "error_text": f"Connection error: {e}"
        }
    
    except ApiIdInvalid as e:
        # Логируем ошибку при подключении к аккаунту
        logging.error(f"ValueError: Ошибка при подключении к аккаунту {from_account}: {e}")
        return {
            "status_code": 2,
            "error_text": f"Connection error: {e}"
        }
    
    except ConnectionError as e:
        # Логируем ошибку при подключении к аккаунту
        logging.error(f"Ошибка при подключении к аккаунту {from_account}: {e}")
        return {
            "status_code": 2,
            "error_text": f"Connection error: {e}"
        }
    except OperationalError as e:
        logging.warning(f"Попытка двойного подключения, переотправка от {from_account} кому {to_account}")
        if error_index >= 10:
            logging.error(f"OperationalError: Блокировка базы данных, вероятная ошибка дублирование соедененных сессий {from_account}")
            return {
                "status_code": 2,
                "error_text": f"Connection Error: {e}"
            }
        return await send(from_account, to_account, text, error_index + 1)
            
    except Exception as e:
        # Логируем любую другую ошибку при подключении к аккаунту
        logging.error(f"Ошибка при подключении к аккаунту {from_account}: {e}")
        return {
            "status_code": 2,
            "error_text": f"Connection error: {e}"
        }
async def send_cron():
    """
    Асинхронная функция для периодической проверки и отправки сообщений.

    Функция работает в бесконечном цикле, проверяя наличие новых сообщений в базе данных.
    Если сообщения найдены, они отправляются, а их статус обновляется в зависимости от результата отправки.
    После обработки всех сообщений функция засыпает на заданный интервал времени.

    INTERVAL_SEC = 60 интервал через который будет следующая итерация проверки
    """
    INTERVAL_SEC = 60 #интервал проверки крона

    while True:
        # Получаем новые сообщения из базы данных, где статус равен None - тоесть новые и не пройденые строки
        
        
        # Обрабатываем каждое сообщение
        async for index, message in messages_db.get_lines(
            key="status",
            value=None
        ):
            # Отправляем сообщение
            result = await send(
                from_account=str(message.get("phone_sender")),
                to_account=str(message.get("phone_recipient")),
                text=message.get("message_text")
            )
            
            # Если сообщение успешно отправлено, обновляем статус и дату отправки
            if result.get("status_code") == 1:
                create_task(
                    messages_db.update_cells(
                        line_num=index,
                        columns=["status", "date_sent"],
                        values=[1, datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
                    )
                )
            # Если произошла ошибка при отправке, обновляем статус и текст ошибки
            elif result.get("status_code") == 2:
                create_task(
                    messages_db.update_cells(
                        line_num=index,
                        columns=["status", "error_text"],
                        values=[2, result.get("error_text")]
                    )
                )
        
        # Засыпаем на заданный интервал времени перед следующей проверкой
        await sleep(INTERVAL_SEC)