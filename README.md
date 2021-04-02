Migrate objects from one MinIO to another or move objects one level up in MinIO

# Top level commands

```
NAME:
  moveobject - Migration tool to move/copy objects to MinIO

USAGE:
  moveobject COMMAND [COMMAND FLAGS | -h] [ARGUMENTS...]

COMMANDS:
  migrate  copy objects from one MinIO to another
  move     move objects up one level
  help, h  Shows a list of commands or help for one command
  
FLAGS:
  --help, -h     show help
  --version, -v  print the version
```

List of objects stored in the file object_listing.txt is generated using
  
`mc ls -r --json ALIAS/BUCKET | jq -r .key`
  
  
## migrate
```
NAME:
  moveobject migrate - copy objects from one MinIO to another

USAGE:
  moveobject migrate [--skip, --fake]

FLAGS:
   --insecure, -i          disable TLS certificate verification
   --log, -l               enable logging
   --debug                 enable debugging
   --data-dir value        data directory
   --skip value, -s value  number of entries to skip from input file (default: 0)
   --fake                  perform a fake migration
   --help, -h              show help
   

EXAMPLES:
1. Migrate objects in "object_listing.txt" to MinIO.
   $ export MINIO_ENDPOINT=https://minio:9000
   $ export MINIO_ACCESS_KEY=minio
   $ export MINIO_SECRET_KEY=minio123
   $ export MINIO_SOURCE_ENDPOINT=https://minio-src:9000
   $ export MINIO_SOURCE_ACCESS_KEY=minio
   $ export MINIO_SOURCE_SECRET_KEY=minio123
   $ export MINIO_BUCKET=miniobucket
   $ moveobject migrate --data-dir /tmp/ 

2. Migrate objects in "object_listing.txt" from one MinIO tp another after skipping 100000 entries in this file
   $ export MINIO_ENDPOINT=https://minio:9000
   $ export MINIO_ACCESS_KEY=minio
   $ export MINIO_SECRET_KEY=minio123
   $ export MINIO_SOURCE_ENDPOINT=https://minio-src:9000
   $ export MINIO_SOURCE_ACCESS_KEY=minio
   $ export MINIO_SOURCE_SECRET_KEY=minio123
   $ export MINIO_BUCKET=miniobucket
   $ moveobject migrate --data-dir /tmp/ --skip 10000

3. Perform a dry run for migrating objects in "object_listing.txt" from one MinIO to another
   $ export MINIO_ENDPOINT=https://minio:9000
   $ export MINIO_ACCESS_KEY=minio
   $ export MINIO_SECRET_KEY=minio123
   $ export MINIO_SOURCE_ENDPOINT=https://minio-src:9000
   $ export MINIO_SOURCE_ACCESS_KEY=minio
   $ export MINIO_SOURCE_SECRET_KEY=minio123
   $ export MINIO_BUCKET=miniobucket
   $ moveobject migrate --data-dir /tmp/ --fake --log
```

## move
```
NAME:
   moveobject move - move objects up one level
 
 USAGE:
   moveobject move [--skip, --fake]
 
 FLAGS:
  --insecure, -i          disable TLS certificate verification
  --log, -l               enable logging
  --debug                 enable debugging
  --data-dir value        data directory
  --skip value, -s value  number of entries to skip from input file (default: 0)
  --fake                  perform a fake migration
  --help, -h              show help
  
 
 EXAMPLES:
 1. Move objects in "object_listing.txt" in MinIO.
  $ export MINIO_ENDPOINT=https://minio:9000
  $ export MINIO_ACCESS_KEY=minio
  $ export MINIO_SECRET_KEY=minio123
  $ export MINIO_BUCKET=miniobucket
  $ moveobject move --data-dir /tmp/
 
 2. Move objects in "object_listing.txt" in MinIO after skipping 100000 entries in this file
  $ export MINIO_ENDPOINT=https://minio:9000
  $ export MINIO_ACCESS_KEY=minio
  $ export MINIO_SECRET_KEY=minio123
  $ export MINIO_BUCKET=miniobucket
  $ moveobject move --data-dir /tmp/ --skip 10000
 
 3. Perform a dry run for moving objects in "object_listing.txt" in MinIO
  $ export MINIO_ENDPOINT=https://minio:9000
  $ export MINIO_ACCESS_KEY=minio
  $ export MINIO_SECRET_KEY=minio123
  $ export MINIO_BUCKET=miniobucket
  $ moveobject move --data-dir /tmp/ --fake --log
```