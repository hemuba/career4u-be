from collections import OrderedDict

cache = OrderedDict()
CAP = 3
def get_sum(x, y):
    if (x, y) in cache:
        print("HIT: taken from cache")
        cache.move_to_end((x, y))
        return cache[(x, y)]
    else:
        cache[(x, y)] = x+y
        cache.move_to_end((x, y))
        print("MISS: adding to cache")
        if len(cache) > CAP:
            cache.popitem(last=False)
            
        return x + y


print(get_sum(3, 5))
print(get_sum(3, 8))
print(get_sum(3, 5))
print(get_sum(3, 4))
print(get_sum(3, 5))
print(get_sum(3, 8))
print(get_sum(4, 5))
print(get_sum(3, 45))
print(get_sum(3, 11))
print(get_sum(318, 8))
print(get_sum(3, 1))
print(get_sum(66, 4))
print(get_sum(9, 5))
print(get_sum(3, 4))
print(get_sum(3, 5))
print(get_sum(3, 8))
print(get_sum(3, 5))
print(get_sum(3, 4))
print(get_sum(3, 5))
print(get_sum(3, 8))
print(get_sum(4, 5))
print(get_sum(3, 45))
print(get_sum(3, 11))
print(get_sum(318, 8))
print(get_sum(3, 1))
print(get_sum(66, 4))
print(get_sum(9, 5))
print(get_sum(3, 4))
