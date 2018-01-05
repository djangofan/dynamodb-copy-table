# dynamodb-copy-table
A simple python script to copy dynamodb table

NOTE: enhanced to work with dynamodb-local

---

### Requirements

- Python 2.x
- boto (`pip install boto`)

### Usage

A simple usage example:

```shell
$ python dynamodb-copy-table.py events-table copyof-events-table http://192.168.99.100:8000 true
```

You can use the environment variables `AWS_DEFAULT_REGION` and `DISABLE_DATACOPY` to select the region and disable the copying of data from source table to destination table.

The default region is `local` for use with dynamodb-local with sharedDb enabled.

```shell
$ AWS_DEFAULT_REGION=us-west-2 DISABLE_DATACOPY=no python dynamodb-copy-table.py events-table copyof-events-table autoset false
```

### References

- [Import and Export DynamoDB Data using AWS Data Pipeline](http://docs.aws.amazon.com/datapipeline/latest/DeveloperGuide/dp-importexport-ddb.html)
- [Original script](https://gist.github.com/iomz/9774415) - had to modify and add support for tables with only hash key
