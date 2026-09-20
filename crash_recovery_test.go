/*
 * SPDX-FileCopyrightText: © 2017-2025 Istari Digital, Inc.
 * SPDX-License-Identifier: Apache-2.0
 */

package badger

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/stretchr/testify/require"
)

// crashTestEnv marks the helper process that plays the role of the crashed
// writer in the crash-recovery tests below.
const crashTestEnv = "BADGER_CRASH_WRITER"

func crashTestOptions(dir string) Options {
	opt := getTestOptions(dir)
	opt.SyncWrites = false
	// Force sizeable values into the value log.
	opt.ValueThreshold = 32
	opt.ValueLogFileSize = 1 << 20
	return opt
}

// crashWriterProcess is the "crashed" half of the crash-recovery tests. It
// opens the DB, writes a few keys, records the value log offset right after
// the first one, and then exits WITHOUT calling Close — the equivalent of
// kill -9 in the middle of the write path.
func crashWriterProcess(t *testing.T) {
	dir := os.Getenv("BADGER_CRASH_DIR")
	db, err := Open(crashTestOptions(dir))
	require.NoError(t, err)

	bigVal := func(b byte) []byte { return bytes.Repeat([]byte{b}, 1024) }
	set := func(key string, val []byte) {
		require.NoError(t, db.Update(func(txn *Txn) error {
			return txn.SetEntry(NewEntry([]byte(key), val))
		}))
	}

	// This key is fully written to the value log; its vlog entry is below the
	// offset we keep, so it must survive the simulated crash.
	set("durable-key", bigVal('d'))

	// Record the vlog offset after the durable entry. The parent process
	// truncates the vlog here to simulate a crash in which the memtable WAL
	// reached disk but the (independently buffered) vlog tail did not.
	off := db.vlog.woffset()
	require.NoError(t, os.WriteFile(
		filepath.Join(dir, "crash_vlog_offset"), []byte(strconv.Itoa(int(off))), 0644))

	// This key's value lands in the vlog tail that the crash wipes out,
	// while its WAL entry (carrying the value pointer) survives.
	set("lost-key", bigVal('l'))

	// This key is small enough to live entirely inside the WAL, so it does
	// not depend on the vlog at all and must survive.
	set("wal-key", []byte("wal-value"))

	// Do NOT Close: kill -9. Buffered mmap writes stay in the page cache,
	// so the parent sees exactly what a crashed machine would have flushed.
	os.Exit(0)
}

// TestCrashRecoveryDanglingValuePointer reproduces the kill -9 window where
// the memtable WAL survives but the value log tail does not. Before the fix,
// WAL replay resurrected value pointers into the lost vlog tail, so reads of
// recently written keys failed with checksum/EOF errors ("value log 校验失败")
// after reopening. After the fix, such writes are treated as lost (which an
// unsynced write is allowed to be), durable keys remain readable, and Open
// succeeds.
func TestCrashRecoveryDanglingValuePointer(t *testing.T) {
	if os.Getenv(crashTestEnv) == "1" {
		crashWriterProcess(t)
		return
	}

	dir, err := os.MkdirTemp("", "badger-crash-test")
	require.NoError(t, err)
	defer removeDir(dir)

	// Phase 1: run the writer and "kill -9" it mid-stream.
	cmd := exec.Command(os.Args[0], "-test.run", "^TestCrashRecoveryDanglingValuePointer$")
	cmd.Env = append(os.Environ(),
		crashTestEnv+"=1",
		"BADGER_CRASH_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "crashed writer failed: %s", out)

	// Phase 2: simulate the divergent flush — the WAL survived, the vlog
	// tail beyond the recorded offset did not.
	offBytes, err := os.ReadFile(filepath.Join(dir, "crash_vlog_offset"))
	require.NoError(t, err)
	off, err := strconv.ParseInt(string(offBytes), 10, 64)
	require.NoError(t, err)
	require.NoError(t, os.Truncate(filepath.Join(dir, "000001.vlog"), off))
	require.NoError(t, os.Remove(filepath.Join(dir, "crash_vlog_offset")))

	// Phase 3: reopen. Open itself must not fail.
	db, err := Open(crashTestOptions(dir))
	require.NoError(t, err, "Open after crash must not report value log corruption")
	defer func() { require.NoError(t, db.Close()) }()

	// The fully durable key is still there.
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("durable-key"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, bytes.Repeat([]byte{'d'}, 1024), val)
			return nil
		})
	}))

	// The WAL-only key is still there.
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("wal-key"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, []byte("wal-value"), val)
			return nil
		})
	}))

	// The key whose vlog tail was lost must be cleanly absent — not a
	// checksum/EOF error from a dangling value pointer.
	err = db.View(func(txn *Txn) error {
		_, err := txn.Get([]byte("lost-key"))
		return err
	})
	require.ErrorIs(t, err, ErrKeyNotFound,
		"lost write must read as not-found, not as value log corruption")

	// The DB is fully usable after recovery.
	require.NoError(t, db.Update(func(txn *Txn) error {
		return txn.SetEntry(NewEntry([]byte("after-crash"), bytes.Repeat([]byte{'a'}, 1024)))
	}))
	require.NoError(t, db.View(func(txn *Txn) error {
		item, err := txn.Get([]byte("after-crash"))
		require.NoError(t, err)
		return item.Value(func(val []byte) error {
			require.Equal(t, bytes.Repeat([]byte{'a'}, 1024), val)
			return nil
		})
	}))
}

// TestCrashRecoverySyncedWritesSurvive pins the other half of the contract:
// with SyncWrites every acknowledged write must survive kill -9, and reopen
// must find it intact.
func TestCrashRecoverySyncedWritesSurvive(t *testing.T) {
	if os.Getenv(crashTestEnv) == "1" {
		dir := os.Getenv("BADGER_CRASH_DIR")
		opt := crashTestOptions(dir)
		opt.SyncWrites = true
		db, err := Open(opt)
		require.NoError(t, err)
		for i := 0; i < 8; i++ {
			key := fmt.Sprintf("synced-%d", i)
			require.NoError(t, db.Update(func(txn *Txn) error {
				return txn.SetEntry(NewEntry([]byte(key), bytes.Repeat([]byte{byte('0' + i)}, 512)))
			}))
		}
		os.Exit(0) // kill -9: no Close
	}

	dir, err := os.MkdirTemp("", "badger-crash-test")
	require.NoError(t, err)
	defer removeDir(dir)

	cmd := exec.Command(os.Args[0], "-test.run", "^TestCrashRecoverySyncedWritesSurvive$")
	cmd.Env = append(os.Environ(),
		crashTestEnv+"=1",
		"BADGER_CRASH_DIR="+dir,
	)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, "crashed writer failed: %s", out)

	opt := crashTestOptions(dir)
	opt.SyncWrites = true
	db, err := Open(opt)
	require.NoError(t, err)
	defer func() { require.NoError(t, db.Close()) }()

	for i := 0; i < 8; i++ {
		key := fmt.Sprintf("synced-%d", i)
		require.NoError(t, db.View(func(txn *Txn) error {
			item, err := txn.Get([]byte(key))
			require.NoError(t, err, "synced write %q lost after crash", key)
			return item.Value(func(val []byte) error {
				require.Equal(t, bytes.Repeat([]byte{byte('0' + i)}, 512), val)
				return nil
			})
		}))
	}
}
