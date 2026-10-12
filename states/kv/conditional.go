package kv

import (
	"context"
	"fmt"

	clientv3 "go.etcd.io/etcd/client/v3"

	"github.com/milvus-io/birdwatcher/models"
)

// RevisionedValue is an exact preimage read at a consistent etcd revision.
type RevisionedValue struct {
	Key         string
	Value       []byte
	ModRevision int64
}

type ConditionalChange struct {
	Before RevisionedValue
	After  []byte
}

// PrefixRevision also fences new keys, such as a newly published schema.
type PrefixRevision struct {
	Prefix   string
	Revision int64
}

// ConditionalKV is optional: metadata repairs must not fall back to sequential
// Save calls on backends that cannot atomically check and update all records.
type ConditionalKV interface {
	LoadPrefixAtRevision(context.Context, string, int64) ([]RevisionedValue, int64, error)
	CommitChanges(context.Context, []RevisionedValue, []PrefixRevision, []ConditionalChange) error
}

func (kv *etcdKV) LoadPrefixAtRevision(ctx context.Context, prefix string, revision int64) ([]RevisionedValue, int64, error) {
	opts := []clientv3.OpOption{clientv3.WithPrefix()}
	if revision > 0 {
		opts = append(opts, clientv3.WithRev(revision))
	}
	resp, err := kv.client.Get(ctx, joinPath(kv.rootPath, prefix), opts...)
	if err != nil {
		return nil, 0, err
	}
	if revision == 0 {
		revision = resp.Header.Revision
	}
	values := make([]RevisionedValue, 0, len(resp.Kvs))
	for _, entry := range resp.Kvs {
		values = append(values, RevisionedValue{Key: string(entry.Key), Value: entry.Value, ModRevision: entry.ModRevision})
	}
	return values, revision, nil
}

func (kv *etcdKV) CommitChanges(ctx context.Context, guards []RevisionedValue, prefixes []PrefixRevision, changes []ConditionalChange) error {
	if len(changes) == 0 {
		return nil
	}
	checks := make(map[string]int64, len(guards)+len(changes))
	for _, guard := range guards {
		if guard.ModRevision <= 0 {
			return fmt.Errorf("missing revision for %s", guard.Key)
		}
		checks[guard.Key] = guard.ModRevision
	}
	ops := make([]clientv3.Op, 0, len(changes))
	seen := make(map[string]bool, len(changes))
	for _, change := range changes {
		before := change.Before
		if seen[before.Key] || before.ModRevision <= 0 {
			return fmt.Errorf("duplicate change or missing revision for %s", before.Key)
		}
		if rev, ok := checks[before.Key]; ok && rev != before.ModRevision {
			return fmt.Errorf("conflicting revisions for %s", before.Key)
		}
		seen[before.Key] = true
		checks[before.Key] = before.ModRevision
		ops = append(ops, clientv3.OpPut(before.Key, string(change.After)))
	}
	// Stay within etcd's default transaction limit; never split a repair.
	if len(checks)+len(prefixes)+len(ops) > 128 {
		return fmt.Errorf("schema repair exceeds the single-transaction limit")
	}
	cmps := make([]clientv3.Cmp, 0, len(checks)+len(prefixes))
	for key, revision := range checks {
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key), "=", revision))
	}
	for _, prefix := range prefixes {
		if prefix.Revision <= 0 {
			return fmt.Errorf("missing snapshot revision for prefix %s", prefix.Prefix)
		}
		key := joinPath(kv.rootPath, prefix.Prefix)
		cmps = append(cmps, clientv3.Compare(clientv3.ModRevision(key).WithPrefix(), "<", prefix.Revision+1))
	}
	resp, err := kv.client.Txn(ctx).If(cmps...).Then(ops...).Commit()
	if err != nil {
		return fmt.Errorf("schema transaction failed (outcome may be unknown; read back before retrying): %w", err)
	}
	if !resp.Succeeded {
		return fmt.Errorf("metadata changed since preview; no schema repair was written")
	}
	return nil
}

func (c *FileAuditKV) LoadPrefixAtRevision(ctx context.Context, prefix string, revision int64) ([]RevisionedValue, int64, error) {
	cli, ok := c.cli.(ConditionalKV)
	if !ok {
		return nil, 0, fmt.Errorf("backend does not support conditional metadata repairs")
	}
	return cli.LoadPrefixAtRevision(ctx, prefix, revision)
}

func (c *FileAuditKV) CommitChanges(ctx context.Context, guards []RevisionedValue, prefixes []PrefixRevision, changes []ConditionalChange) error {
	cli, ok := c.cli.(ConditionalKV)
	if !ok {
		return fmt.Errorf("backend does not support conditional metadata repairs")
	}
	// The caller saves a checked, durable backup before commit. Keep the regular
	// audit stream as well, using the actual preimage for each successful put.
	if err := cli.CommitChanges(ctx, guards, prefixes, changes); err != nil {
		return err
	}
	for _, change := range changes {
		c.writeHeader(models.AuditOpType_OpPut, 2)
		c.writeHeader(models.AuditOpType_OpPutBefore, 1)
		c.writeKeyValue(change.Before.Key, string(change.Before.Value))
		c.writeHeader(models.AuditOpType_OpPutAfter, 1)
		c.writeKeyValue(change.Before.Key, string(change.After))
	}
	return nil
}
