Edited history of commands I used to get superset going locally:

 ```
 9594  2024-09-28 16:23  export FLASK_APP=superset
# Required a unique SECRET_KEY generated here
 9604* 2024-09-28 16:25  openssl rand -base64 42
# Created superset_config.py.  Later used a templated version
 9609* 2024-09-28 16:27  cat >superset_config.py
 9612* 2024-09-28 16:29  export SUPERSET_CONFIG_PATH=$PWD/superset_config.py
 9613* 2024-09-28 16:29  pixi run superset fab create-admin
 9614* 2024-09-28 16:30  pixi run superset db upgrade
 9616* 2024-09-28 16:30  pixi run superset init
 9618* 2024-09-28 16:32  pixi run superset db upgrade
 9620* 2024-09-28 16:32  less superset_config.py
# Problems with superset versions
# I had to remove old migration files from my pixi environment before re-running the pixi install
# Some actions not logged
 9639* 2024-09-28 16:42  pixi remove --pypi apache-superset
 9642* 2024-09-28 16:42  pixi add --pypi apache-superset

# I'm not entirely sure this is the right order.  Many intermediate steps cut
 9658* 2024-09-28 16:54  superset db upgrade
 9659* 2024-09-28 16:54  superset fab create-admin
 9660* 2024-09-28 16:54  superset load_examples
 9661* 2024-09-28 16:56  superset init
 
 9662* 2024-09-28 16:56  superset run -p 8088 --with-threads --reload --debugger

# Requires duckdb SQLAlchemy engine
 9663* 2024-09-28 16:59  pixi add duckdb_engine
 9664* 2024-09-28 16:59  pixi add --pypi  duckdb_engine
 9665* 2024-09-28 16:59  superset run -p 8088 --with-threads --reload --debugger
```