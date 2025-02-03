import logging
import pandas as pd
from sys import exit
logger = logging.getLogger(__name__)



class DB:
    """
    Класс для представления базы данных из csv

    Атрибуты
    --------
    __conn: DataFrame
        объект соеденения к базе данных
    """
    def __init__(self,path:str):
        """
        Устанавливает соедение с базой данных

        Параметры
        ---------
        path:str
            назначение csv файла относительно текущей директории, в текущей указать только название
        
        """
        self.__path = path
        self.__conn:pd.DataFrame = None
        try:
            self.__conn = pd.read_csv(path)
            logger.info('db connected/updated')
        except FileNotFoundError as e:
            logger.error(f"FileNotFoundError: path: {path}. bd does`t connect, description: {e}")
            exit()
        except ValueError as e:
            logger.error(f"ValueError: path: {path}. bd does`t connect, description: {e}")
            exit()
        except Exception as e:
            logger.error(f"path: {path}. unidentified error: {e}")
            exit()
    
        
    
    
    async def get_lines(self,key:str = None,value:str | int | bool | None = None,exists:bool=True):
        """
        Получение данных из базы данных по параметру

        Параметры
        ---------
        key:str (default = None)
            Ключ для выборки по столбцу из таблицы
        value:str | int | bool (default = None)
            Значение для проверки выборки строк из массива данных
        exists:bool (default = True)
            Выборка по принадлежности (True) или непринадлежности (False) столбца данных к значению value

        Возвращаемое значение
        ---------------------
        AsyncGenerator[tuple[Hashable, Series], Any]
        """
        self.__init__(self.__path)
        data = None
        
        if value == None:
            data = self.__conn[self.__conn[key].isna()]
        elif exists:
            data = self.__conn[self.__conn[key] == value]
        elif not exists:
            data = self.__conn[self.__conn[key] != value]
        else:
            data = self.__conn
        for index, line in data.iterrows():
            yield index, line
    async def update_cells(self,line_num:int,columns:list,values:list)->None:
        """
        Обновление строк в базе данных

        Параметры
        ---------
        line_num:int
            Индекс строки для обновления
        columns:list
            колонки для обновления
        values:list 
            Значения которые будут вставляться в выбранный диапазон

        Возвращаемое значение
        ---------------------
        None
        """
        self.__init__(self.__path)
        self.__conn.loc[line_num, columns] = values
        self.__conn.to_csv(self.__path, index=False)
        
messages_db = DB("message_sender.csv")
accounts_db = DB("accounts.csv")
