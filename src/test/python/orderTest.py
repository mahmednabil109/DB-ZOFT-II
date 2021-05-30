from string import ascii_lowercase as alc
from itertools import product as prod

def fact(n):
    f = 1
    while n >= 1:
        f *= n
        n -= 1
    return f

def getRank(str):
    n, t_count = len(str), 1

    for i in range(n):
        less_than = 0
        
        for j in range(i + 1, n):
            if ord(str[i]) > ord(str[j]):
                less_than += 1
        
        d_count = [0] * 52

        for j in range(i, n):
            if(ord(str[j]) >= ord('A')) and (ord(str[j]) <= ord('Z')):
                d_count[ord(str[j]) - ord('A')] += 1
            else:
                d_count[ord(str[j]) - ord('a') + 26] += 1
        
        d_fact = 1

        for ele in d_count:
            d_fact *= fact(ele)

        t_count += (fact(n - i - 1) * less_than) / d_fact

    return int(t_count)

if __name__ == "__main__":
    print(getRank("abba"))