from functools import wraps


def cache_me(facility_generator):
    ''' caches output of a function in a database, returns cached data. '''
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            nonlocal facility_generator
            facility = facility_generator()
            facility.update(func=func)
            facility.hash(args, kwargs)
            success, data = facility.search()
            if success:
                return data
            data = func(*args, **kwargs)
            facility.cache(data=data)
            facility.clean()
            return data
        return wrapper
    return decorator

# cache = cache_me(system=IntakeCache(...))
