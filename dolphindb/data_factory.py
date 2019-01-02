from struct import Struct
from .date_util import *
from dolphindb.socket_util import read_string, recvall, recvallhex
from .pair import Pair
from .settings import *
from .type_util import *
import numpy as np
import pandas as pd


def get_form_type(socket, buffer):
    flag = DATA_UNPACKER_SCALAR[DT_SHORT](socket, buffer)
    data_form = flag >> 8
    data_type = flag & 0xff
    return data_form, data_type


def table_str_col_generator(socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    vc = np.array([read_string(socket, buffer) for i in range(size)])
    return vc


def read_dolphindb_obj_general(socket, buffer):
    data_form, data_type = get_form_type(socket, buffer)
    if data_form == DF_VECTOR and data_type == DT_ANY:
        return VECTOR_FACTORY[DT_ANY](socket, buffer)
    elif data_form in [DF_SCALAR, DF_VECTOR]:

        if data_type in DATA_LOADER[data_form]:
            obj = DATA_LOADER[data_form][data_type](socket, buffer)
            if data_type == DT_BOOL:
                if data_form == DF_SCALAR:
                    if isinstance(obj, nan) or obj is None or np.isnan(obj):
                        return boolNan
                    return bool(obj)
                else:
                    obj_new = []
                    for o in obj:
                        if isinstance(o, nan) or o is None or np.isnan(o):
                            obj_new.append(boolNan)
                        else:
                            obj_new.append(bool(o))
                    obj = obj_new
            return obj
        else:
            return None
    elif data_form in [DF_SET, DF_DICTIONARY, DF_TABLE, DF_MATRIX]:
        return DATA_LOADER[data_form](socket, buffer)
    elif data_form in [DF_PAIR]:
        return Pair.fromlist(DATA_LOADER[data_form][data_type](socket, buffer))
    else:
        return None

def vec_generator(socket, data_type, buffer):
    '''
    generate a numpy array from a dolphindb vector
    :param socket: TCP socket
    :param data_type: dolphindb data type
    :return: the python corresponding data type
    '''
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    if data_type in [DT_SYMBOL, DT_STRING]:
        # print("size",size)
        """
        while True:
            packet = recvall(socket, 4096)
            if not packet or not len(packet):
                break
            data += packet
        (data.split('\x00\x00')[0].split('\x00')[:size])
        """
        return [read_string(socket, buffer) for i in range(size)]
    else:
        return list(DATA_UNPACKER[data_type](socket, size, buffer))

def vec_generator_df(socket, data_type, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    if data_type in [DT_SYMBOL, DT_STRING]:
        return [read_string(socket, buffer) for i in range(size)]
    else:
        return list(DATA_UNPACKER[data_type](socket, size, buffer))


def vector_factory_any(socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    # read one more byte, otherwise fail to generate the vector, not sure why
    # DATA_UNPACKER_SCALAR[DT_BYTE](socket)
    size = row * col
    myList = []
    for i in range(0, size):
        myList.append(read_dolphindb_obj_general(socket, buffer))
    return myList


def set_generator(socket, buffer):
    data_form, data_type = get_form_type(socket, buffer)
    if data_type == DT_VOID:
        return set([])
    if ( data_form != DF_VECTOR):
        raise RuntimeError("The form of set keys must be vector")
    vec = VECTOR_FACTORY[data_type](socket, buffer)
    return set(vec)


def dict_generator(socket, buffer):

    """ read key array """
    key_form, key_type = get_form_type(socket, buffer)
    if key_form != DF_VECTOR:
        raise Exception("The form of dictionary keys must be vector")
    if key_type < 0 or key_type >= TYPE_NUM:
        raise Exception("Invalid key type: " + str(key_type))

    keys = VECTOR_FACTORY[key_type](socket, buffer)

    """ read value array """
    val_form, val_type = get_form_type(socket, buffer)
    if val_form != DF_VECTOR:
        raise Exception("The form of dictionary keys must be vector")
    if val_type < 0 or val_type >= TYPE_NUM:
        raise Exception("Invalid key type: " + str(key_type))
    vals = VECTOR_FACTORY[val_type](socket, buffer)

    if len(keys) != len(vals):
        raise Exception("The keys array size is not equal to the vals array size.")
    tmp = dict()
    for idx in range(len(keys)):
        tmp[keys[idx]] = vals[idx]
    return tmp


def _symbol_handler(data_type, socket, buffer):
    return table_str_col_generator(socket, buffer)


def _int_handler(data_type, socket, buffer):
    return VECTOR_FACTORY[DT_INT](socket, buffer)


def _bool_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [val is 1 if val == 1 or val == 0 else boolNan for val in DATA_UNPACKER[DT_BOOL](socket, size, buffer)]


def _date_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'D') for val in DATA_UNPACKER[DT_DATE](socket, size, buffer)]


def _month_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [np.datetime64(val - 1970 * 12 if val >= 0 else None, 'M') for val in DATA_UNPACKER[DT_MONTH](socket, size, buffer)]


def _datetime_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 's') for val in DATA_UNPACKER[DT_DATETIME](socket, size, buffer)]


def _timestamp_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'ms') for val in DATA_UNPACKER[DT_TIMESTAMP](socket, size, buffer)]


def _time_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'ms') for val in DATA_UNPACKER[DT_TIME](socket, size, buffer)]


def _second_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 's') for val in DATA_UNPACKER[DT_SECOND](socket, size, buffer)]


def _minute_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'm') for val in DATA_UNPACKER[DT_MINUTE](socket, size, buffer)]


def _nanotime_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'ns') for val in DATA_UNPACKER[DT_NANOTIME](socket, size, buffer)]


def _nanotimestamp_handler(data_type, socket, buffer):
    row = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    col = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = row * col
    return [np.datetime64(val if val >= 0 else None, 'ns') for val in DATA_UNPACKER[DT_NANOTIMESTAMP](socket, size, buffer)]


def _default_handler(data_type, socket, buffer):
    return VECTOR_FACTORY[data_type](socket, buffer)


TABLE_GEN_HANDLER = dict()
TABLE_GEN_HANDLER[DT_INT] = _int_handler
TABLE_GEN_HANDLER[DT_BOOL] = _bool_handler
TABLE_GEN_HANDLER[DT_DATE] = _date_handler
TABLE_GEN_HANDLER[DT_MONTH] = _month_handler
TABLE_GEN_HANDLER[DT_DATETIME] = _datetime_handler
TABLE_GEN_HANDLER[DT_TIMESTAMP] = _timestamp_handler
TABLE_GEN_HANDLER[DT_TIME] = _time_handler
TABLE_GEN_HANDLER[DT_SECOND] = _second_handler
TABLE_GEN_HANDLER[DT_MINUTE] = _minute_handler
TABLE_GEN_HANDLER[DT_NANOTIME] = _nanotime_handler
TABLE_GEN_HANDLER[DT_NANOTIMESTAMP] = _nanotimestamp_handler
TABLE_GEN_HANDLER[DT_SYMBOL] = _symbol_handler
TABLE_GEN_HANDLER[DT_STRING] = _symbol_handler

def table_generator(socket, buffer):
    """
    Generate a pandas data frame from dolphindb table object
    :param socket:
    :return:
    """
    rows = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    cols = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    tableName = read_string(socket, buffer)
    """ read column names """
    colNameDict = dict()
    colNames = []
    for i in range(cols):
        name = read_string(socket, buffer)
        colNameDict[name] = len(colNameDict)
        colNames.append(name)
    """ read columns and generate a pandas data frame"""
    df = pd.DataFrame()
    for col in colNames:
        data_form, data_type = get_form_type(socket, buffer)
        # print(data_type)
        if data_form != DF_VECTOR:
            raise Exception("column " + col + "in table " + tableName + " must be a vector!")

        df[col] = TABLE_GEN_HANDLER.get(data_type, _default_handler)(data_type, socket, buffer)
        # print(df)
    return df


def matrix_generator(socket, buffer):
    hasLabel = DATA_UNPACKER_SCALAR[DT_BYTE](socket, buffer)
    rowLabels = None
    colLabels = None
    if hasLabel & 1 == 1:
        data_form, data_type = get_form_type(socket, buffer)
        if data_form != DF_VECTOR:
            raise Exception("The form of matrix row labels must be vector")
        if data_type < 0 or data_type >= TYPE_NUM:
            raise Exception("Invalid data type for matrix row labels: " + str(data_type))
        rowLabels = VECTOR_FACTORY[data_type](socket, buffer)

    if hasLabel & 2 == 2:
        data_form, data_type = get_form_type(socket, buffer)
        if data_form != DF_VECTOR:
            raise Exception("The form of matrix row labels must be vector")
        if data_type < 0 or data_type >= TYPE_NUM:
            raise Exception("Invalid data type for matrix row labels: " + str(data_type))
        colLabels = VECTOR_FACTORY[data_type](socket, buffer)

    flag = DATA_UNPACKER_SCALAR[DT_SHORT](socket, buffer)
    # print(flag)
    data_type = flag & 0xff
    if data_type < 0 or data_type >= TYPE_NUM:
        raise Exception("Invalid data type for matrix row labels: " + str(data_type))
    rows = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    cols = DATA_UNPACKER_SCALAR[DT_INT](socket, buffer)
    size = rows * cols

    # print(data_type)
    # print(type(DATA_UNPACKER[data_type](socket, size)))
    vals = DATA_UNPACKER[data_type](socket, size, buffer)
    if vals is not None:
        # print(data_type,socket)
        vals = np.transpose(np.array(list(vals)).reshape(cols,rows))
    if not len(vals):
        vals = None
    return vals, rowLabels, colLabels


"""endiness: the function is reset in dolphindb.connect"""
endianness = lambda x : x

""" Unpack scalar from dolphindb object """
DATA_UNPACKER_SCALAR = dict()
DATA_UNPACKER_SCALAR[DT_VOID] = lambda x, y: swap_fromxxdb(Struct('b').unpack(recvall(x, DATA_SIZE[DT_BOOL], y))[0], DT_BOOL)
DATA_UNPACKER_SCALAR[DT_BOOL] = lambda x, y: swap_fromxxdb(Struct('b').unpack(recvall(x, DATA_SIZE[DT_BOOL], y))[0], DT_BOOL)
DATA_UNPACKER_SCALAR[DT_BYTE] = lambda x, y: swap_fromxxdb(Struct('b').unpack((recvall(x, DATA_SIZE[DT_BYTE], y)))[0], DT_BYTE)
DATA_UNPACKER_SCALAR[DT_SHORT] = lambda x, y: swap_fromxxdb(Struct(endianness('h')).unpack(recvall(x, DATA_SIZE[DT_SHORT], y))[0], DT_SHORT)
DATA_UNPACKER_SCALAR[DT_INT] = lambda x, y: swap_fromxxdb(Struct(endianness('i')).unpack((recvall(x, DATA_SIZE[DT_INT], y)))[0], DT_INT)
DATA_UNPACKER_SCALAR[DT_LONG] = lambda x, y: swap_fromxxdb(Struct(endianness('q')).unpack((recvall(x, DATA_SIZE[DT_LONG], y)))[0], DT_LONG)
DATA_UNPACKER_SCALAR[DT_DATE] = lambda x, y: Date(Struct(endianness('i')).unpack((recvall(x, DATA_SIZE[DT_DATE], y)))[0])
DATA_UNPACKER_SCALAR[DT_MONTH] = lambda x, y: Month(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_MONTH], y))[0])
DATA_UNPACKER_SCALAR[DT_TIME] = lambda x, y: Time(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_TIME], y))[0])
DATA_UNPACKER_SCALAR[DT_MINUTE] = lambda x, y: Minute(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_MINUTE], y))[0])
DATA_UNPACKER_SCALAR[DT_SECOND] = lambda x, y: Second(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_SECOND], y))[0])
DATA_UNPACKER_SCALAR[DT_DATETIME] = lambda x, y: Datetime(Struct(endianness('i')).unpack(recvall(x, DATA_SIZE[DT_DATETIME], y))[0])
DATA_UNPACKER_SCALAR[DT_TIMESTAMP] = lambda x, y: Timestamp(Struct(endianness('q')).unpack((recvall(x, DATA_SIZE[DT_TIMESTAMP], y)))[0])
DATA_UNPACKER_SCALAR[DT_NANOTIME] = lambda x, y: NanoTime(Struct(endianness('q')).unpack(recvall(x, DATA_SIZE[DT_NANOTIME], y))[0])
DATA_UNPACKER_SCALAR[DT_NANOTIMESTAMP] = lambda x, y: NanoTimestamp(Struct(endianness('q')).unpack((recvall(x, DATA_SIZE[DT_NANOTIMESTAMP], y)))[0])
DATA_UNPACKER_SCALAR[DT_DATETIME64] = lambda x, y: NanoTimestamp(Struct(endianness('q')).unpack(recvall(x, DATA_SIZE[DT_NANOTIMESTAMP], y))[0])
DATA_UNPACKER_SCALAR[DT_FLOAT] = lambda x, y: swap_fromxxdb(Struct(endianness('f')).unpack(recvall(x, DATA_SIZE[DT_FLOAT], y))[0], DT_FLOAT)
DATA_UNPACKER_SCALAR[DT_DOUBLE] = lambda x, y: swap_fromxxdb(Struct(endianness('d')).unpack((recvall(x, DATA_SIZE[DT_DOUBLE], y)))[0], DT_DOUBLE)
DATA_UNPACKER_SCALAR[DT_SYMBOL] = lambda x, y: read_string(x, y)
DATA_UNPACKER_SCALAR[DT_STRING] = lambda x, y: read_string(x, y)
DATA_UNPACKER_SCALAR[DT_ANY] = lambda x, y: None
DATA_UNPACKER_SCALAR[DT_DICTIONARY] = lambda x, y: None
DATA_UNPACKER_SCALAR[DT_OBJECT] = lambda x, y: None

DATA_UNPACKER = dict()
DATA_UNPACKER[DT_VOID] = lambda x, y, z: map(lambda z: swap_fromxxdb(z, DT_BOOL), Struct(str(y)+'b').unpack(recvall(x, DATA_SIZE[DT_BOOL]*y, z)))
DATA_UNPACKER[DT_BOOL] = lambda x, y, z: map(lambda z: swap_fromxxdb(z, DT_BOOL), Struct(str(y)+'b').unpack(recvall(x, DATA_SIZE[DT_BOOL]*y, z)))
DATA_UNPACKER[DT_BYTE] = lambda x, y, z: map(lambda z: swap_fromxxdb(z, DT_BYTE), Struct(str(y)+'b').unpack(recvall(x, DATA_SIZE[DT_BYTE]*y, z)))
DATA_UNPACKER[DT_SHORT] = lambda x, y, z: map(lambda z: swap_fromxxdb(z, DT_SHORT), Struct(endianness(str(y)+'h')).unpack(recvall(x, DATA_SIZE[DT_SHORT]*y, z)))
DATA_UNPACKER[DT_INT] = lambda x, y, z: list(map(lambda z: swap_fromxxdb(z, DT_INT), Struct(endianness(str(y)+'i')).unpack(recvall(x, DATA_SIZE[DT_INT]*y, z))))
DATA_UNPACKER[DT_LONG] = lambda x, y, z: map(lambda z: swap_fromxxdb(z, DT_LONG), Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_LONG]*y, z))))
DATA_UNPACKER[DT_DATE] = lambda x, y, z: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_DATE]*y, z)))
DATA_UNPACKER[DT_MONTH] = lambda x, y, z: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_MONTH]*y, z)))
DATA_UNPACKER[DT_TIME] = lambda x, y, z: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_TIME]*y, z)))
DATA_UNPACKER[DT_MINUTE] = lambda x, y, z: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_MINUTE]*y, z)))
DATA_UNPACKER[DT_SECOND] = lambda x, y, z: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_SECOND]*y, z)))
DATA_UNPACKER[DT_DATETIME] = lambda x, y, z: Struct(endianness(str(y)+'i')).unpack((recvall(x, DATA_SIZE[DT_DATETIME]*y, z)))
DATA_UNPACKER[DT_TIMESTAMP] = lambda x, y, z: Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_TIMESTAMP]*y, z)))
DATA_UNPACKER[DT_NANOTIME] = lambda x, y, z: Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_NANOTIME]*y, z)))
DATA_UNPACKER[DT_NANOTIMESTAMP] = lambda x, y, z: Struct(endianness(str(y)+'q')).unpack((recvall(x, DATA_SIZE[DT_NANOTIMESTAMP]*y, z)))
DATA_UNPACKER[DT_DATETIME64] = lambda x, y, z: Struct(endianness(str(y)+'q')).unpack(recvall(x, DATA_SIZE[DT_NANOTIMESTAMP]*y, z))
DATA_UNPACKER[DT_FLOAT] = lambda x, y, z: map(lambda z: swap_fromxxdb(z, DT_FLOAT), Struct(endianness(str(y)+'f')).unpack(recvall(x, DATA_SIZE[DT_FLOAT]*y, z)))
DATA_UNPACKER[DT_DOUBLE] = lambda x, y, z: list(map(lambda z: swap_fromxxdb(z, DT_DOUBLE), Struct(endianness(str(y)+'d')).unpack((recvall(x, DATA_SIZE[DT_DOUBLE]*y, z)))))
DATA_UNPACKER[DT_SYMBOL] = lambda x, y, z: None
DATA_UNPACKER[DT_STRING] = lambda x, y, z: None
DATA_UNPACKER[DT_ANY] = lambda x, y, z: None
DATA_UNPACKER[DT_DICTIONARY] = lambda x, y, z: None
DATA_UNPACKER[DT_OBJECT] = lambda x, y, z: None

""" dictionary of functions for making numpy arrays from dolphindb vectors"""
VECTOR_FACTORY = dict()
VECTOR_FACTORY[DT_VOID] = lambda x, y:[]
VECTOR_FACTORY[DT_BOOL] = lambda x, y: vec_generator(x, DT_BOOL, y)
VECTOR_FACTORY[DT_BYTE] = lambda x, y: vec_generator(x, DT_BYTE, y)
VECTOR_FACTORY[DT_SHORT] = lambda x, y: vec_generator(x, DT_SHORT, y)
VECTOR_FACTORY[DT_INT] = lambda x, y: vec_generator(x, DT_INT, y)
VECTOR_FACTORY[DT_LONG] = lambda x, y: vec_generator(x, DT_LONG, y)
VECTOR_FACTORY[DT_DATE] = lambda x, y: list(map(Date, vec_generator(x, DT_DATE, y)))
VECTOR_FACTORY[DT_MONTH] = lambda x, y: list(map(Month, vec_generator(x, DT_MONTH, y)))
VECTOR_FACTORY[DT_TIME] = lambda x, y: list(map(Time, vec_generator(x, DT_TIME, y)))
VECTOR_FACTORY[DT_MINUTE] = lambda x, y: list(map(Minute, vec_generator(x, DT_MINUTE, y)))
VECTOR_FACTORY[DT_SECOND] = lambda x, y: list(map(Second, vec_generator(x, DT_SECOND, y)))
VECTOR_FACTORY[DT_DATETIME] = lambda x, y: list(map(Datetime, vec_generator(x, DT_DATETIME, y)))
VECTOR_FACTORY[DT_TIMESTAMP] = lambda x, y: list(map(Timestamp, vec_generator(x, DT_TIMESTAMP, y)))
VECTOR_FACTORY[DT_NANOTIME] = lambda x, y: list(map(NanoTime, vec_generator(x, DT_NANOTIME, y)))
VECTOR_FACTORY[DT_NANOTIMESTAMP] = lambda x, y: list(map(NanoTimestamp, vec_generator(x, DT_NANOTIMESTAMP, y)))
VECTOR_FACTORY[DT_DATETIME64] = lambda x, y: list(map(NanoTimestamp, vec_generator(x, DT_DATETIME64, y)))
VECTOR_FACTORY[DT_FLOAT] = lambda x, y: vec_generator(x, DT_FLOAT, y)
VECTOR_FACTORY[DT_DOUBLE] = lambda x, y: vec_generator(x, DT_DOUBLE, y)
VECTOR_FACTORY[DT_SYMBOL] = lambda x, y: list(map(lambda z: swap_fromxxdb(z, DT_SYMBOL), vec_generator(x, DT_SYMBOL, y)))
VECTOR_FACTORY[DT_STRING] = lambda x, y: list(map(lambda z: swap_fromxxdb(z, DT_STRING), vec_generator(x, DT_STRING, y)))
VECTOR_FACTORY[DT_ANY] = vector_factory_any

""" dictionary of functions for loading different forms of data from dolphindb api"""
DATA_LOADER = dict()
DATA_LOADER[DF_SCALAR] = DATA_UNPACKER_SCALAR
DATA_LOADER[DF_VECTOR] = VECTOR_FACTORY
DATA_LOADER[DF_PAIR] = VECTOR_FACTORY
DATA_LOADER[DF_SET] = lambda x, y: set_generator(x, y)
DATA_LOADER[DF_DICTIONARY] = lambda x, y: dict_generator(x, y)
DATA_LOADER[DF_TABLE] = lambda x, y: table_generator(x, y)
DATA_LOADER[DF_MATRIX] = lambda x, y: matrix_generator(x, y)

""" pack from python scalar"""
DATA_PACKER_SCALAR = dict()
DATA_PACKER_SCALAR[DT_BOOL] = lambda x: Struct('b').pack(swap_toxxdb(x))
DATA_PACKER_SCALAR[DT_SHORT] = lambda x: Struct(endianness('h')).pack(swap_toxxdb(x))
DATA_PACKER_SCALAR[DT_INT] = lambda x: Struct(endianness('i')).pack(swap_toxxdb(x))
DATA_PACKER_SCALAR[DT_LONG] = lambda x: Struct(endianness('q')).pack(swap_toxxdb(x))
DATA_PACKER_SCALAR[DT_DOUBLE] = lambda x: Struct(endianness('d')).pack(swap_toxxdb(x))
DATA_PACKER_SCALAR[DT_STRING] = lambda x: x.encode() + Struct('b').pack(0)
DATA_PACKER_SCALAR[DT_DATE] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_MONTH] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_TIME] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_MINUTE] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_SECOND] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_DATETIME] = lambda x: Struct(endianness('i')).pack(x.value)
DATA_PACKER_SCALAR[DT_TIMESTAMP] = lambda x: Struct(endianness('q')).pack(x.value)
DATA_PACKER_SCALAR[DT_NANOTIME] = lambda x: Struct(endianness('q')).pack(x.value)
DATA_PACKER_SCALAR[DT_NANOTIMESTAMP] = lambda x: Struct(endianness('q')).pack(x.value)
DATA_PACKER_SCALAR[DT_DATETIME64] = lambda x: x.tobytes()
# In DT_DATETIME64 packer, the byte array of a nanotimestamp is directly copyied from the underlying memory of datetime64
# datetime64 has a identical memory layout with time types in DolphinDB
# DATA_PACKER_SCALAR[DT_DATETIME64] = lambda x: Struct(endianness('q')).pack(x)

""" pack from numpy 1D array """
DATA_PACKER = dict()
DATA_PACKER[DT_BOOL] = lambda x: Struct(endianness("%db" % x.size)).pack(*map(lambda y: swap_toxxdb(y), x))
DATA_PACKER[DT_INT] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: swap_toxxdb(y), x))
DATA_PACKER[DT_LONG] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: swap_toxxdb(y), x))
DATA_PACKER[DT_DOUBLE] = lambda x: Struct(endianness("%dd" % x.size)).pack(*map(lambda y: swap_toxxdb(y), x))
DATA_PACKER[DT_STRING] = lambda x: (''.join(map(lambda y: y+'\x00', x))).encode()
DATA_PACKER[DT_DATE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_MONTH] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_TIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_MINUTE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_SECOND] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_DATETIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_TIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_NANOTIME] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_NANOTIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))
DATA_PACKER[DT_DATETIME64] = lambda x: b''.join(list(map(lambda y: y.tobytes(), x)))
# DATA_PACKER[DT_DATETIME64] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y: y.value, x))

""" pack from numpy multi-dimensional array """
DATA_PACKER2D = dict()
DATA_PACKER2D[DT_BOOL] = lambda x: Struct(endianness("%db" % x.size)).pack(*map(lambda y:swap_toxxdb(y), x.T.flat))
DATA_PACKER2D[DT_INT] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:swap_toxxdb(y), x.T.flat))
DATA_PACKER2D[DT_LONG] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:swap_toxxdb(y), x.T.flat))
DATA_PACKER2D[DT_DOUBLE] = lambda x: Struct(endianness("%dd" % x.size)).pack(*map(lambda y: swap_toxxdb(y), x.T.flat))
DATA_PACKER2D[DT_STRING] = None # dolphindb doesn't support 2-D string matrix
DATA_PACKER2D[DT_DATE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_MONTH] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_TIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_MINUTE] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_SECOND] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_DATETIME] = lambda x: Struct(endianness("%di" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_TIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_NANOTIME] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_NANOTIMESTAMP] = lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
DATA_PACKER2D[DT_DATETIME64] = lambda x: b''.join(list(map(lambda y: y.tobytes(), x.T.flat)))
# DATA_PACKER2D[DT_DATETIME64] =lambda x: Struct(endianness("%dq" % x.size)).pack(*map(lambda y:y.value, x.T.flat))
