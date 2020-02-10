'''
subclass CacheFacility in order to change caching functionality.
produce your own facility function using your new CacheFacility class.
shouldn't need to touch cache_me.
apply in manner below
'''

from cache_it import cache_me
from cache_it import facility


cache_it = cache_me(facility)


@cache_it
def test_cache_a():
    something = test_cache_b()
    print('a', something + something)
    return something + something


@cache_it
def test_cache_b():
    something = test_cache_c()
    print('b', something + 1)
    return something + 1


@cache_it
def test_cache_c():
    print('c', 1)
    return 1


def test_cache():
    return test_cache_a()


test_cache()
