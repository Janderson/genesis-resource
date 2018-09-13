import ctypes
from ctypes import *
DLL_PATH = "D:/desenvolvimento/ls/genesis-backend/old/profitdll.dll"
from os.path import exists
#hllDll = ctypes.WinDLL (DLL_PATH)
#print(exists(DLL_PATH))
CDLL(DLL_PATH)