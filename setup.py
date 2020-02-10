from setuptools import setup, find_packages

with open("readme.md", "r") as fh:
    LONG_DESCRIPTION = fh.read()

NAME = 'cache_it'
VERSION = '2020.02.10'

setup(
    name=NAME,
    version=VERSION,
    namespace_packages=[NAME],
    description='caches functions automatically.',
    long_description=LONG_DESCRIPTION,
    long_description_content_type="text/markdown",
    packages=[f'{NAME}.{p}' for p in find_packages(where=NAME)],
    install_requires=[
        'pandas',
        'dask',
        'boxset',
    ],
    python_requires='>=3.5.2',
    author='Jordan Miller',
    author_email='jordan.kay@gmail.com',
    url='https://github.com/propername/cache_it',
    # classifiers=[
    #     "Programming Language :: Python :: 3",
    #     "License :: OSI Approved :: Apache Software License",
    #     "Operating System :: OS Independent",
    # ],
)
