from itertools import product as pro
from string import ascii_lowercase as l
import math

def convert(t):
    _t = ''
    while t > 0:
        _t += chr(97 + t % 26)
        t //= 26
    return _t
print(convert(211))
exit(0)
print(l)

res = list(pro(l, l, repeat=2))
res2 = list(pro(l, l, repeat=1))

print(res2[211], len(res2))
t = 0
for i in range(4):
    t += (ord(res[211][3 - i]) - 97) * 26 ** i

print(t, res[211])

_t = ''
default = math.ceil(math.log(t + 26 ** len('a' * (len(res[0]) -1))) / math.log(26))
while t > 0:
    _t += chr(97 + t % 26)
    t //= 26
print(default)
while _t.__len__() < default:
        _t += 'a'

print(_t[::-1])

"""                
                  t = 211 => adi, aadi, aaaaadi
         t = 211 => di , default = 3   adi
                                0  26^3 - 1      2
    aa - zz,                 aaa - zzz,         aaaa - zzzz
   0   26^2-1, 


    default = 3

   0.2      1.23
 0  25  26    26^2 - 1
 a - z , aa - zz , aaa - zzz, aaaa - zzzz
 log(n) 
    26  
log(n) / log(26)   

"""


"""
    assuming that characters is from "` a b c d e  ...x  y  z" where ` is invisable
                                      0 1 2 3 4 5   ..   25  26
"""
# def convert2_27(s):
#     t = 0
#     for i in range(len(s)):
#         t += ( ord(s[len(s) - 1 - i]) - ord('`')) * 27 ** i
#     return t

# def convert2_str(t):
#     _t = ''
#     while t > 0:
#         _t += chr( t%27 + ord('`'))
#         t //= 27
#     return _t[::-1]

# s1, s2, t1, t2, _t1, _t2 = 'zz', 'aaa', 0, 0, '', ''

# t1, t2 = convert2_27(s1), convert2_27(s2)

# print(
#     convert2_str(t1),
#     convert2_str(t1 + 1),
#     convert2_str(t1 + 29),
#     convert2_str(t2)
#     )
