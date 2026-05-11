package storage

import "github.com/cockroachdb/pebble"

// Tuned for append-only time-series ingest: memtable size matches L0 target
// so each flush produces exactly one SST; uniform 64MB SSTs across all levels
// (per RocksDB default: file size stays constant, level capacity grows
// geometrically via LBaseMaxBytes × LevelMultiplier); Zstd only at the
// bottommost level (per RocksDB tuning guide) — Pebble's dynamic level bytes
// places ~90% of data at the bottom level, so Zstd applies to most of the DB.
func tunedPebbleOptions(epoch uint32) *pebble.Options {
	listener := NewPebbleEventListener(epoch)
	return &pebble.Options{
		EventListener:            &listener.EventListener,
		MemTableSize:             64 * 1024 * 1024,  // 64MB
		LBaseMaxBytes:            512 * 1024 * 1024, // 512MB
		MaxConcurrentCompactions: func() int { return 4 },
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 64 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L0 64MB
			{TargetFileSize: 64 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L1 64MB
			{TargetFileSize: 64 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L2 64MB
			{TargetFileSize: 64 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L3 64MB
			{TargetFileSize: 64 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L4 64MB
			{TargetFileSize: 64 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L5 64MB
			{TargetFileSize: 64 * 1024 * 1024, Compression: pebble.ZstdCompression},   // L6 64MB
		},
	}
}
