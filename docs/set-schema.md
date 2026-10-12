# Set an Array or StructArray capacity

`set schema` repairs the outer `max_capacity` of one top-level logical field in
RootCoord and the latest persisted StreamingNode schema of every collection
shard. Use it with metadata writers stopped and outstanding schema DDL resolved.
It does not issue an online Milvus DDL or stop/restart the cluster.

In the connected instance state:

```text
set schema --collectionID <collection-id> --field lawyer --max-capacity 4096
```

This is a dry-run. It prints the affected field IDs, old/new capacities, exact
metadata keys and their revisions. To execute the same repair:

```text
set schema --collectionID <collection-id> --field lawyer --max-capacity 4096 --run
```

For the StructArray `lawyer` (ID 131), all four members (IDs 132–135) are updated
together, for example from 1000 to 4096. Their `max_length`, nullable attributes,
names, IDs and other parameters are preserved. Select the parent `lawyer`;
`lawyer.party_name` is not interpreted as a member path.

## Supported changes

- A top-level scalar Array: update its `type_params.max_capacity`.
- A top-level nested Array: update only the outer capacity and its existing
  legacy mirror. Inner capacities remain unchanged.
- A StructArray: update the outer capacity of every scalar/vector member, and
  any existing parent capacity parameter.

Values must be in **1–4096**. Decreases are rejected. If all current copies
already contain the requested value, the command performs no writes.
The configured limits of the target Milvus deployment must also permit the
requested capacity.

## Persistence and consistency

- RootCoord split field records and embedded collection schemas are supported.
  When a matching SuffixSnapshot exists, only its newest version is updated
  alongside the live value; older snapshots are preserved.
- For each mapped shard, StreamingNode schema keys are ordered by their numeric
  checkpoint timestamp. Only the newest schema record is modified. Historical
  schema records are not rewritten or removed.
- The newest schema must have been published by the vchannel checkpoint. An
  unpublished newer record, missing shard, schema version mismatch or differing
  target definition causes the entire repair to fail before writing.
- Field IDs, schema versions, WAL contents and checkpoints are unchanged.
- All changes use one etcd transaction with revision checks, including checks
  for newly added schemas. The command does not fall back to sequential writes.
  Unsupported backends/layouts and oversized transactions are rejected.

Before writing, a mode-0600 JSON backup records the original bytes, replacement
bytes and revisions. By default it is created as `schema-backup-*.json` in the
current directory. Choose a new path explicitly with:

```text
set schema --collectionID <collection-id> --field lawyer --max-capacity 4096 --backup repair-lawyer.json --run
```

An existing backup file is never overwritten. The command reads back the written
records before reporting success. A transport failure may leave the transaction
outcome unknown; inspect the recorded keys before retrying. Backups do not imply
an unconditional restore is safe after subsequent legitimate metadata changes.

## Local verification

The schema tests cover the 131–135 field structure, two shards with multiple
schema versions, preservation of unknown protobuf fields, dry-run/no-op,
snapshot layouts, audit wrappers and all-or-nothing conflict handling.
Integration tests use an explicit test etcd endpoint and a unique temporary
prefix, which is removed after each test:

```sh
BIRDWATCHER_SCHEMA_TEST_ETCD=127.0.0.1:12379 go test ./states/etcd/set -run '^TestSchema' -count=1
```

Without that environment variable, only the pure tests run; the etcd tests skip.
