"""
Функции для чтения мета-данных из Parquet файлов

Изначально - минимально достаточный набор, будет, видимо, расширяться
"""

import struct
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport

import inspect
import types
import yaml

from IPython.display import display, Markdown

EXTRA_HDR = "Дополнительная информация (см. также help()):\n\n"
EXTRA_DOCS = None # загрузим при первом вызове

def showExtra(obj):
    """ Выводит значение атрибута extra класса obj в виде Markdown в юпитере
    триммит слева пробелы """

    global EXTRA_DOCS

    # if EXTRA_DOCS is None:
    if True: # пока разрабатываю документацию буду всегда перегружать
        with open('extra_docs.yaml', 'r') as file:
            EXTRA_DOCS = yaml.safe_load(file)

    className = type(obj).__name__
    if className in EXTRA_DOCS:
        display(Markdown(EXTRA_HDR+EXTRA_DOCS[className]))    
    else:
        print("(для этого класса нет дополнительной информации...)")

def getFooterLen(fPath):
    """ функция для получения длины футера (чтобы каждый раз это не вспоминать) """
    
    with open(fPath, "rb") as file:
        file.seek(-8, 2) # последние 4 байта = PAR1, перед ними - длина футера
        return struct.unpack("<i", file.read(4))[0]

def decodeObject(filePath, offset, obj, nBytes = 10000):
    """ читает объект из thrift-файла filePath (предполагая использование TCompactProtocol)
    * offset: смещение объекта, если отрицательное - от конца файла
    * obj: тип возвращаемого объекта (в thrift спецификациях у всех объектов конструктор можно вызвать без параметров)
    * nBytes: как правило, в этом протоколе структуры заканчиваются маркером, поэтому можно читать больше, чем нужно
    
    Возвращает объект заданного типа.
    """
    
    with open(filePath, "rb") as file:
        if offset>=0:
            file.seek(offset)
        else:
            file.seek(offset,2)
        objBytes = file.read(nBytes)
        transport = TTransport.TMemoryBuffer(objBytes)
        protocol = TCompactProtocol.TCompactProtocol(transport)
        objInstance = obj()
        objInstance.read(protocol)
        return objInstance

def getTobjSize(obj):
    """  способ понять размер thrift объекта в файле (TCompactProtocol) """
    
    transport = TTransport.TMemoryBuffer()
    protocol = TCompactProtocol.TCompactProtocol(transport)
    
    obj.write(protocol) # пишем объект в память
    transport.flush() # на всякий случай
    return len(transport.getvalue()) # размер объекта в байтах

def is_scalar(obj):
    """GPT: Check if an object is a scalar."""
    return isinstance(obj, (int, float, str, bool, bytes, bytearray, memoryview, type(None)))

def format_member_name(name, member):
    """GPT: Format member name based on its type."""
    if isinstance(member, list):
        return name + f'[{len(member)}]'
    elif inspect.ismethod(member):
        return name + '()'
    elif is_scalar(member):
        return f"{name}: {member}"
    else:
        return name + "#"

def getLevelStr(what):
    """ Возвращает строку с одним "уровнем" детализации атрибутов объекта:

    * для атрибутов объекта типа list выводится длина списка
        * чтобы вывести содержимое списка - используем ее же
    * значения скалярных атрибутов выводятся (после двоеточия)
    * атрибуты, являющиеся объектами, помечены суффиксом "#"
    * атрибуты-методы - суффиксом "()"  
    """
    
    if isinstance(what,list):
        return str(what)
        
    resList = ["First level elements of " + str(type(what))]
    for name, member in inspect.getmembers(what):
        if not name.startswith('_'):
            resList.append(format_member_name(name, member))

    return "\n".join(resList)