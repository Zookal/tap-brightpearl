# tap-brightpearl


Brightpearl API retrieves a multitude of data on different ways.

This code try its best to get the data from Brightpearl on more generic way as possible.

Brightpearl `GET` and `SEARCH` endpoint for some entities retrieves the same dataset. The search normally is fast and most used.   

Some objects are dependent of another API query, the code highlight it when it is needed 

#TODO: 
* need to read value: object - product custom value 
* readme
* code clean up
* make PIP
* own repo
* counter is doubling



generating catalog one off or when changes:

add changes on Stream 
```bash
pip install singer-python
cd extras/tap-brighpearl/
export PYTHONPATH=$(pwd):$PYTHONPATH

python tap_brightpearl/__init__.py -c a.json -d > brightpearl_catalog.json

python tap_brightpearl/__init__.py -c a.json --catalog brightpearl_catalog.json

~/.virtualenvs/target-snowflake/bin/target-snowflake -c /tmp/brightpearl_snowflake.json
```