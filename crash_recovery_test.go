/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/dgraph-io/badger/v4/y"
)

// crashTestOptions returns options tuned for crash-recovery tests: small
// files so copies are cheap, a tiny value threshold so values land in the
// value log, and no background sync/flush surprises.
func crashTestOptions(dir string) Options {
	opt := DefaultOptions(dir)
	opt.ValueThreshold = 8
	opt.VLogPercentile = 0
	opt.MemTableSize = 1 << 20
	opt.ValueLogFileSize = 1 << 20
	opt.SyncWrites = false
	opt.CompactL0OnClose = false
	opt.DetectConflicts = false
	return opt
}

// copyDBFiles copies every regular file of a (still open) DB directory into
// dst, mimicking the on-disk state a crash would leave behind: whatever the
// OS had in the page cache is visible, but nothing is closed or truncated
// the way a clean Close would do it.
func copyDBFiles(t *testing.T, src, dst string) {
	t.Helper()
	require.NoError(t, os.MkdirAll(dst, 0o755))
	entries, err := os.ReadDir(src)
	require.NoError(t, err)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		require.NoError(t, err)
		require.NoError(t, os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644))
	}
}

// vptrOf returns the value log pointer stored for key in the DB.
func vptrOf(t *testing.T, db *DB, key string) valuePointer {
	t.Helper()
	vs, err := db.get(y.KeyWithTs([]byte(key), math.MaxUint64))
	require.NoError(t, err)
	require.True(t, vs.Meta&bitValuePointer > 0, "expected %q to be stored as a value pointer", key)
	var vp valuePointer
	vp.Decode(vs.Value)
	return vp
}

func mustGet(t *testing.T, db *DB, key string, want []byte) {
	t.Helper()
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte(key))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, want, val)
			return nil
		})
	}))
}

func getErr(db *DB, key string) error {
	return db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte(key))
		return err
	})
}

// TestCrashRecoveryDanglingVlogTail simulates a crash where the WAL made it
// to disk but the tail of the value log did not: the WAL holds a value
// pointer whose target was never persisted. Reopening must not surface the
// half-written entry; the key must simply be absent, while fully persisted
// keys must remain readable.
func TestCrashRecoveryDanglingVlogTail(t *testing.T) {
	dir := t.TempDir()
	opt := crashTestOptions(dir)
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	val1 := []byte(strings.Repeat("a", 100))
	val2 := []byte(strings.Repeat("b", 100))
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("k1"), val1) }))

	// k2's vlog entry starts at the current write offset; remember it.
	k2Off := int64(db.vlog.woffset())
	require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("k2"), val2) }))
	vp2 := vptrOf(t, db, "k2")
	require.Equal(t, uint32(k2Off), vp2.Offset)

	// Make only the WAL durable, then "crash": copy the directory as-is and
	// chop off the unflushed vlog tail containing k2's value.
	require.NoError(t, db.mt.SyncWAL())
	crashDir := filepath.Join(t.TempDir(), "crash")
	copyDBFiles(t, dir, crashDir)
	require.NoError(t, os.Truncate(vlogFilePath(crashDir, vp2.Fid), k2Off))

	db2, err := Open(crashTestOptions(crashDir))
	require.NoError(t, err, "crash recovery must open the directory")
	defer func() { require.NoError(t, db2.Close()) }()

	mustGet(t, db2, "k1", val1)
	require.ErrorIs(t, getErr(db2, "k2"), ErrKeyNotFound,
		"entry whose value never reached disk must not come back")
}

// TestCrashRecoveryTornVlogEntry simulates a torn write: the vlog region the
// WAL points to is still zero-filled (pages never flushed), while the file
// itself retains its preallocated length. Before the fix this surfaced as a
// checksum mismatch (with VerifyValueChecksum) or, worse, as a silently
// decoded garbage value. Recovery must drop the dangling entry instead.
func TestCrashRecoveryTornVlogEntry(t *testing.T) {
	for _, verify := range []bool{false, true} {
		name := "garbage-value"
		if verify {
			name = "checksum-mismatch"
		}
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			opt := crashTestOptions(dir)
			opt.VerifyValueChecksum = verify
			db, err := Open(opt)
			require.NoError(t, err)
			defer func() { require.NoError(t, db.Close()) }()

			val1 := []byte(strings.Repeat("a", 100))
			val2 := []byte(strings.Repeat("b", 100))
			require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("k1"), val1) }))
			require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte("k2"), val2) }))
			vp2 := vptrOf(t, db, "k2")

			require.NoError(t, db.mt.SyncWAL())
			crashDir := filepath.Join(t.TempDir(), "crash")
			copyDBFiles(t, dir, crashDir)

			// Zero out k2's vlog entry: the file keeps its length, but the
			// content never made it to disk.
			f, err := os.OpenFile(vlogFilePath(crashDir, vp2.Fid), os.O_WRONLY, 0o644)
			require.NoError(t, err)
			_, err = f.WriteAt(make([]byte, vp2.Len), int64(vp2.Offset))
			require.NoError(t, err)
			require.NoError(t, f.Close())

			db2, err := Open(crashTestOptions(crashDir))
			require.NoError(t, err, "crash recovery must open the directory")
			defer func() { require.NoError(t, db2.Close()) }()

			mustGet(t, db2, "k1", val1)
			require.ErrorIs(t, getErr(db2, "k2"), ErrKeyNotFound,
				"torn value must not be returned as a valid value")
		})
	}
}

// TestCrashRecoverySyncedEntriesSurvive makes sure the recovery-side
// validation does not drop entries whose value log data is intact: anything
// that was durable before the crash must still be there after reopening.
func TestCrashRecoverySyncedEntriesSurvive(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(crashTestOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	vals := map[string][]byte{
		"k1": []byte(strings.Repeat("a", 100)),
		"k2": []byte(strings.Repeat("b", 100)),
		"k3": []byte(strings.Repeat("c", 100)),
	}
	for k, v := range vals {
		require.NoError(t, db.Update(func(txn *Txn) error { return txn.Set([]byte(k), v) }))
	}
	// A full Sync (vlog + WAL) makes everything durable.
	require.NoError(t, db.Sync())

	crashDir := filepath.Join(t.TempDir(), "crash")
	copyDBFiles(t, dir, crashDir)

	db2, err := Open(crashTestOptions(crashDir))
	require.NoError(t, err)
	defer func() { require.NoError(t, db2.Close()) }()
	for k, v := range vals {
		mustGet(t, db2, k, v)
	}
}

// TestMemtableFlushSyncsValueLog pins the flush-side ordering: a memtable
// flush persists value pointers into an L0 table (after which the WAL is
// discarded), so the value log must be synced first. Otherwise a crash can
// leave an SST pointing at value log data that never reached disk.
func TestMemtableFlushSyncsValueLog(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(crashTestOptions(dir))
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	synced := make(chan struct{}, 1)
	db.flushVlogSyncHook = func() { synced <- struct{}{} }

	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.Set([]byte("k"), []byte(strings.Repeat("v", 100)))
	}))
	require.NoError(t, db.handleMemTableFlush(db.mt, nil))

	select {
	case <-synced:
	case <-time.After(10 * time.Second):
		t.Fatal("value log was not synced before flushing the memtable to L0")
	}
}
