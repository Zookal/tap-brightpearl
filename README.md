# tap-brightpearl

BrightPearl is an ERP system (www.brightpearl.com)

Brightpearl API retrieves a multitude of data on different ways and the documentation is on: https://api-docs.brightpearl.com.

This code try its best to get the data from Brightpearl on more generic way as possible.

Brightpearl `GET` and `SEARCH` endpoint for some entities retrieves the same dataset, but the GET is more detailed for some objects. The search normally is fast.   

Some objects are dependent of another API query, the code highlight it when it is needed (Orders, Goods NOtes IN/Out, Product Price, Product with Custom data).

## incremental load
There are 2 types of incremental loads on the system:

1 - directly from the entity (stream table) nased on specific field

2 - indirectly based on a search for some objects 

The indirectly is the most complicated due to the dependency of builds URI list with filtering values for the next endpoint call.
On this indirect call (depending_on), if there is no state file, a call from the endpoint idset is made, it is fast object output. However, if the state file is setup for the entity (stream), the search endpoint is used because it allows filtering.

BrightPearl does not have a good design on updated date attributes (updatedOn field), making it a bit trick to trust the source sometimes. To overcome this problem, there is config value `incremental_back_days` which can be used to control how much back the date will be before filtering the data.    

It is a good idea to refresh your dataset every now and them from BrightPearl, just don't set the state file for those objects for a full data retrieval.

## Product Order IDSET to help with data deletion

Product and Order IDSet are implemented to help support data deletion or other tasks on the environment ends.

It is a pure list of IDs using commas and from-to values (1,3,5-9,11,13-15) up to 200 objects. It can save a lot of API calls and time.

This can help avoid reloading the whole data set all the time, this table can be used to guide a full reload of data.

## State File Format

```shell script
 {"bookmarks": {"product": {"updatedOn": "2020-10-29T16:55:13.000+11:00"}}}}

 {"bookmarks": {entity: {state_filter: value}, entity2: {state_filter: value}}}
```

## Catalog

In Singer, you need to set the `SELECTED` field on the JSON catalog for the objects required to process.  

Use the current [schemas/schema.json](schemas/schema.json) as a guide or run the discovery from the command line after installation (it take its times, better copy the repo's file):

```bash
tap-brightpearl -c config.json -d > my_catalog.json
```

## Installation

```bash
# download this package locally

pip install .

#or 
pip install https://github.com/Zookal/tap-brightpearl/archives/master.zip
```

## Configuration

This tap requires a `config.json` which authentication and others. See [sample_config.json](sample_sample.json) for an example.

To run `tap-brightpearl` with the configuration file, use this command:

```bash
tap-brightpearl -c my-config.json -s state.json --catalog catalog.json
```

## Changelog
0.0.1 - basic code deployment  





## quick shortcuts for local development
```bash
export PYTHONPATH=$(pwd):$PYTHONPATH
python tap_brightpearl/__init__.py -c config.json -d > bp_catalog.json
python tap_brightpearl/__init__.py -c config.json --catalog bp_catalog.json

```
## TODO
 publish on PIP
