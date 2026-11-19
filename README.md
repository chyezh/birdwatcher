# Birdwatcher

[![linter status](https://github.com/milvus-io/birdwatcher/workflows/golangci-lint/badge.svg)](https://github.com/milvus-io/birdwatcher/actions/workflows/golangci-lint.yml?query=branch%3Amain+)

A debug tool for Milvus 2.0 or later. This tool connects to etcd and inspects some status of the Milvus system.

## Get Started

### Prerequisite
go 1.18 or higher 

### Install birdwatcher

download source and run with go command

```shell
git clone https://github.com/milvus-io/birdwatcher
cd birdwatcher
go install
```

tips: [As JimB notice in comments](https://stackoverflow.com/questions/69807151/go-install-github-com-dmacvicar-terraform-provider-libvirtlatest-shows-error):

> If there are replace or exclude directives in the module, the correct installation method is to clone the source and install it.

## How to use

### connect to etcd 

<img alt="connect to etcd" src="./docs/images/connect.gif" width="600" />

```shell
❯ birdwatcher
# execute connect command, using default config, equal to connect --etcd localhost:2379 --rootPath by-dev
Offline: connect 
Using meta path: by-dev/meta/
✔ Milvus(by-dev): show collection
...
```

### connect to remote instance etcd with other basePath

```shell
❯ birdwatcher
Offline: connect --etcd [ip]:[port] --rootPath instanceName
Using meta path: instanceName/meta/
✔ Milvus(instanceName): 

```

### inspect some meta


<img alt="inspect meta" src="./docs/images/show.gif" width="600" />

```
Milvus(by-dev): show collections
================================================================================
Collection ID: 434393323906469185	Collection Name:gosdk_index_example
Partitions:
 - Partition ID:434393323906469186	Partition Name:_default
Fields:
 - Field ID: 0 	 Field Name: RowID 	 Field Type: Int64
 - Field ID: 1 	 Field Name: Timestamp 	 Field Type: Int64
 - Field ID: 100 	 Field Name: ID 	 Field Type: Int64
	 - Primary Key, AutoID: false
 - Field ID: 101 	 Field Name: Year 	 Field Type: Int32
 - Field ID: 102 	 Field Name: Vector 	 Field Type: FloatVector
	 - Type Param dim: 8
Consistency Level: Strong
✔ Milvus(by-dev): █
```

### Export segment inserts from the WAL

Connect to the source instance and run the following command at the Birdwatcher prompt.

```shell
export-wal --segments 123,456 --topic by-dev-rootcoord-dml_0 --message-id 18/27/-1 --mq-addr 127.0.0.1:6650 --output recovered
```

The start position is inclusive. Pulsar accepts a `ledger/entry/batch` ID, a base64 WAL checkpoint ID, or `earliest`. For Kafka, use a `WKAFKA` build with `--wal kafka` and a decimal partition-0 offset. The reader stops at the topic tail captured when the command starts. Choose a position at or before BeginTxn to include a transaction.

Each matching segment produces a local `<segment-id>.jsonl` file in the new output directory. The command reports row counts, including zero for segments with no matching inserts. The files contain insert history, including inserts from committed upserts. Deletes and compaction are outside the export. Encrypted inserts and legacy row-based blobs produce an error.

Upload each file to the target Milvus object store and pass its object key to bulk import for the corresponding collection and partition. The exporter uses the source collection schema in etcd, retains original primary keys and omits function outputs. To preserve IDs when importing, use a target with a manual primary key or enable `allow_insert_auto_id` on a target version that supports it.

### backup etcd


<img alt="inspect meta" src="./docs/images/backup.gif" width="600" />

```
Milvus(by-dev): backup
found 37 keys, at revision 533816, starting backup...
Backing up ... 100%(37/37)
backup etcd for prefix by-dev/meta done, stored in file: bw_etcd_ALL.220707-152246.bak.gz
```

### help

And use `help` command to check other commands.
