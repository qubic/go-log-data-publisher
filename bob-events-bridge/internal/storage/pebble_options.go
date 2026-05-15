package storage

import "github.com/cockroachdb/pebble"

func tunedPebbleOptions(epoch uint32) *pebble.Options {
	listener := NewPebbleEventListener(epoch)
	options := &pebble.Options{
		EventListener:            &listener.EventListener,
		MemTableSize:             64 * 1024 * 1024,  // 64MB
		LBaseMaxBytes:            512 * 1024 * 1024, // 512MB
		MaxConcurrentCompactions: func() int { return 4 },
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 64 * 1024 * 1024, Compression: pebble.NoCompression},       // L0
			{TargetFileSize: 128 * 1024 * 1024, Compression: pebble.SnappyCompression},  // L1
			{TargetFileSize: 256 * 1024 * 1024, Compression: pebble.SnappyCompression},  // L2
			{TargetFileSize: 512 * 1024 * 1024, Compression: pebble.SnappyCompression},  // L3
			{TargetFileSize: 1024 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L4
			{TargetFileSize: 2048 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L5
			{TargetFileSize: 4096 * 1024 * 1024, Compression: pebble.ZstdCompression},   // L6
		},
	}

	return options
}
