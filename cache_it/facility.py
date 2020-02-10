'''
The cache_me decorator needs a class of functions it can call to complete it's
tasks related to caching logic. This is the default facility that uses the
Sqlite object from boxset.sqlite. One could write others that communicate with
an intake server or whatever.

Now, I'm pretty sure this isn't the best way to do this, but it is really
simple. You can subclass this thing, then give your specialized object it to
the decorator and it will do things the way you want it to do them.

The only requirements are that all the methods accept these arguments, and that
the hash method returns a tuple of strings and the search method return a tuple
of types (bool, anything), and that the bool indicate if data is cached.
'''

import os
import time
import datetime as dt
import pandas as pd
from boxset.aid import coerce
from boxset.aid import string_this
from boxset.aid import unwrap_dataframe
from boxset.hash import hash_any
from boxset.hash import hash_this
from boxset.hash import hash_these
from boxset.meta import strip_code
from boxset.meta import source_code_of
from boxset.sqlite import Sqlite


class CacheFacility():

    def __init__(
        self,
        database: str,
        modules: list = None,
        limit: int = None,
        clear: bool = True,
    ):
        '''
        database - path of database / database folder.
        modules - list of module names such as ['my_projects']
        limit - maximum number of records to be kept in the database
        clear - do you want to clear records made before the code changed?
        '''
        self.modules = coerce(modules, list)
        self.limit = limit
        self.clear = clear
        self.given_database = database

    def update(self, func: callable):
        if not os.path.exists(self.given_database):
            os.makedirs(self.given_database)
        self.func = func
        self.name = f'{func.__module__}.{func.__name__}'
        self.database = os.path.join(
            self.given_database, self.name.replace('.', '') + '.sqlite')

    ###########################################################################
    # hash ####################################################################
    ###########################################################################

    def hash_code(self):
        code = ''.join([
            strip_code(c) for c in source_code_of(self.func, self.modules)])
        return hash_this(data=code)

    def hash_inputs(self, args, kwargs):
        input_hash = ''
        if args:
            args_hash = hash_these(strings=[string_this(a) for a in args])
            input_hash = args_hash
        if kwargs:
            kwargs_hash = hash_any(thing={
                k: string_this(v) for k, v in sorted(kwargs.items())})
            input_hash = kwargs_hash
        if args and kwargs:
            input_hash = hash_these([args_hash, kwargs_hash])
        return input_hash

    def hash(self, args, kwargs):
        self.code_hash = self.hash_code()
        self.input_hash = self.hash_inputs(args, kwargs)

    ###########################################################################
    # search ##################################################################
    ###########################################################################

    def search_local_sqlite(self):
        query = f'''
            select id, data_point
            from registry
            where name = '{self.name}'
            and code = '{self.code_hash}'
            and input = '{self.input_hash}'
        '''
        initialize = f'''
            create table registry (
            [id] INTEGER PRIMARY KEY,
            [name] text,
            [code] text,
            [input] text,
            [timestamp] text,
            [data_point] text)
        '''
        with Sqlite(database=self.database, initialize=initialize) as sql:
            return sql.read(query=query)

    def retrieve_local_sqlite(self, id: str, reference: str) -> tuple:
        ''' it found a match, get the data, return it, update timestamp '''
        with Sqlite(database=self.database) as sql:
            sql.update(
                table='registry', where=f"id={id}",
                columns='timestamp', values=str(dt.datetime.now()))
            data = sql.read(
                query=f"select * from '{reference}'")
        return True, unwrap_dataframe(data)

    def search(self) -> tuple:
        '''
        returns (bool, data)
        bool is an indication of data was found in the cache.
        '''
        df = self.search_local_sqlite()
        if not df.empty:
            return self.retrieve_local_sqlite(
                id=df.loc[0, 'id'],
                reference=df.loc[0, 'data_point'])
        return False, None

    ###########################################################################
    # cache ###################################################################
    ###########################################################################

    def uniuqe_reference(self):
        ''' produces an address for the data to be cached '''
        now = str(time.time()).replace('.', '')
        return f"{self.name.replace('.','')}_{now}"

    def cache_data_local_sqlite(self, data, reference: str):
        ''' caches the data '''
        if not isinstance(data, pd.DataFrame):
            data = pd.DataFrame({'data': [data]})
        with Sqlite(database=self.database) as sql:
            sql.load(
                table=reference,
                data=data)

    def cache_record_local_sqlite(self, reference: str):
        ''' cache record w/ input hashes and current code hash and datetime'''
        with Sqlite(database=self.database) as sql:
            sql.load(
                table='registry',
                data=pd.DataFrame({
                    'name': [self.name],
                    'code': self.code_hash,
                    'input': self.input_hash,
                    'data_point': reference,
                    'timestamp': str(dt.datetime.now())}))

    def cache(self, data):
        ''' function ran and produced this data, now we need to cache it '''
        data_point = self.uniuqe_reference()
        self.cache_data_local_sqlite(data=data, reference=data_point)
        self.cache_record_local_sqlite(reference=data_point)

    ###########################################################################
    # clean ###################################################################
    ###########################################################################

    def clean_obsolete(self):
        ''' clear out outdated items (code has changed) '''
        where = (
            f"(name == '{self.name}' and"
            f" code <> '{self.code_hash}')")
        with Sqlite(database=self.database) as sql:
            data = sql.read(query=(
                f'select data_point from registry where {where}'))
            # delete the data itself
            for ix, row in data.iterrows():
                sql.drop(table=row['data_point'])
            # delete the record
            sql.delete(table='registry', where=where)

    def clean_to_limit(self):
        ''' if limit has been reached remove oldest item '''
        with Sqlite(database=self.database) as sql:
            query = 'select count(*) from registry'
            if unwrap_dataframe(sql.read(query=query)) > self.limit:
                data = sql.read(query=(
                    'select id, data_point from registry where id in ('
                    'select id from registry '
                    'order by timestamp asc limit 1)'))
                # delete the data itself
                sql.drop(table=data.loc[0, 'data_point'])
                # delete the record
                sql.delete(
                    table='registry',
                    where=f"id = {data.loc[0, 'id']}")

    def clean(self):
        ''' clear and limit '''
        if self.clear:
            self.clean_obsolete()
        if self.limit and self.limit > 0:
            self.clean_to_limit()


def facility():
    return CacheFacility(
        database='./database',
        modules=None,
        limit=None,
        clear=True,)
