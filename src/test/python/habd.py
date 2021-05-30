from itertools import product
from string import ascii_lowercase as l

l = l.upper() + l
# res = []
# res = list(product(l))
# res += list(product(l, l))
# res += list(product(l, l, l))
# res = map(''.join, res)
# res = sorted(res)
# _t = '\n'.join(res)
# with open('habd.txt', 'w') as habd:
#     habd.write(_t)


def inc(str):
    res = list((str[:-1] + '@'))
    for i in range(len(str) - 2, -1, -1):
        if res[i] != 'z':
            res[i] = l[l.index(res[i]) + 1]
            break
        else:
            res[i] = '@'
    return ''.join(res)

def reach(base, offset):
    while offset:
        if '@' in base:
            _count = base.count('@')
            base = base.replace('@', 'A', min(_count, offset))
            offset -= min(_count, offset) 
        else:
            if offset >= 52:
                base = inc(base)
                offset -= 52
            else:
                base = base[:-1] + l[l.index(base[-1]) + offset]
                offset = 0
    return base

print(inc('AzA'), reach('A@@', 143363))
# B@@

"""
706
12

base => A@@

dest => ??

aaa

"""