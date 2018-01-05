#!/usr/bin/python2

############
# CMD Example:  AWS_DEFAULT_REGION=us-west-2 DISABLE_DATACOPY=no python dynamodb-copy-table.py events-table copyof-events-table true
# the default region is 'local', change it with the AWS_DEFAULT_REGION or AWS_REGION env variable.
# region does not matter if your dynamodb-local is configured for 'sharedDb'
# supports a DISABLE_DATACOPY option
############

# set local dynamo host
localHost = 'http://192.168.99.100:8000'

from boto.dynamodb2.exceptions import ValidationException
from boto.dynamodb2.fields import HashKey, RangeKey
from boto.dynamodb2.layer1 import DynamoDBConnection
from boto.dynamodb2.table import Table
from boto.exception import JSONResponseError
from time import sleep
import sys
import os

if len(sys.argv) != 4:
    print 'Usage: %s <source_table_name> <destination_table_name> <isLocal>' % sys.argv[0]
    sys.exit(1)

src_table = sys.argv[1]
dst_table = sys.argv[2]
isLocal = sys.argv[3]
tableRegion = os.getenv('AWS_DEFAULT_REGION', os.getenv('AWS_REGION', 'not-set'))
print '*** AWS client region is: ' + tableRegion
print '*** Script running in local mode: ' + isLocal

if not isLocal:
    host = 'dynamodb.%s.amazonaws.com' % region
    ddbc = DynamoDBConnection()
    DynamoDBConnection.DefaultRegionName = tableRegion
else:
    print '*** Using Dynamo host: ' + localHost
    ddbc = DynamoDBConnection(is_secure=False, region=tableRegion, host=localHost)


print '*** Starting copy event ...'
print '  - Will timeout after 2+ minutes if it fails ...'

# 1. Read and copy the target table to be copied
# this will timeout if it fails , after about 2 minutes
table_struct = None
try:
    logs = Table(src_table, connection=ddbc)
    table_struct = logs.describe()
except:
    print 'ERROR: Failure reading table {0} from region {1} in mode {2}'.format(src_table, tableRegion, isLocal)
    sys.exit(1)

print '*** Reading key schema from %s table' % src_table
src = ddbc.describe_table(src_table)['Table']
hash_key = ''
range_key = ''
for schema in src['KeySchema']:
    attr_name = schema['AttributeName']
    key_type = schema['KeyType']
    if key_type == 'HASH':
        hash_key = attr_name
    elif key_type == 'RANGE':
        range_key = attr_name

print '*** Creating new table ...'

# 2. Create the new table
table_struct = None
try:
    new_logs = Table(dst_table,
                     connection=ddbc,
                     schema=[HashKey(hash_key),
                             RangeKey(range_key),
                             ]
                     )

    table_struct = new_logs.describe()
    print 'ERROR: Table %s already exists' % dst_table
    sys.exit(0)
except JSONResponseError:
    schema = [HashKey(hash_key)]
    if range_key != '':
        schema.append(RangeKey(range_key))
    new_logs = Table.create(dst_table,
                            connection=ddbc,
                            schema=schema,
                            )
    print '*** Waiting for the new table %s to become active' % dst_table
    sleep(5)
    while ddbc.describe_table(dst_table)['Table']['TableStatus'] != 'ACTIVE':
        sleep(3)

if 'DISABLE_DATACOPY' in os.environ:
    print 'Copying of data from source table is disabled. Exiting...'
    sys.exit(0)

print '*** Adding items to new table ...'

# 3. Add the items
for item in logs.scan():
    new_item = {}
    new_item[hash_key] = item[hash_key]
    if range_key != '':
        new_item[range_key] = item[range_key]
    for f in item.keys():
        if f in [hash_key, range_key]:
            continue
        new_item[f] = item[f]
    try:
        new_logs.use_boolean()
        new_logs.put_item(new_item, overwrite=True)
    except ValidationException:
        print dst_table, new_item
    except JSONResponseError:
        print ddbc.describe_table(dst_table)['Table']['TableStatus']

print 'SUCCESS: We are done. Exiting...'
