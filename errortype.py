
'''
Author:pengji
Email:jiqunpeng@gmail.com
Time:2013-06-20
'''



class SqlError(Exception):
    def __init__(self,value):
        self.value = value
    def __str__(self):
        return repr(self.value)
NO_SUCH_TABLE = 0x800
NO_SUCH_ATTR = 0x801
