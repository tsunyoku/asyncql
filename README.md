# asyncql
<p>
<a href="https://pypi.org/project/databases/">
    <img src="https://badge.fury.io/py/databases.svg" alt="Package version">
</a>
</p>

An asynchronous python library for all your database needs.

This is designed as an alternative/replacement for [databases](https://github.com/encode/databases), without the use of SQLAlchemy.

## Installation

**Requires Python3.7+**

Currently supported databases:

- [MySQL (with asyncmy)](https://github.com/long2ice/asyncmy)

You can install `asyncql` with your desired database like so:

```bash
$ pip install asyncql[mysql]
```