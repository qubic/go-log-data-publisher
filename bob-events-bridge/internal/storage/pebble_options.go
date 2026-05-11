package storage

import "github.com/cockroachdb/pebble"

func tunedPebbleOptions(epoch uint32) *pebble.Options {
	listener := NewPebbleEventListener(epoch)
	return &pebble.Options{
		EventListener:            &listener.EventListener,
		MemTableSize:             64 * 1024 * 1024,  // 64MB
		LBaseMaxBytes:            256 * 1024 * 1024, // 256MB
		MaxConcurrentCompactions: func() int { return 4 },
		Levels: []pebble.LevelOptions{
			{TargetFileSize: 4 * 1024 * 1024, Compression: pebble.SnappyCompression},  // L0 4MB
			{TargetFileSize: 8 * 1024 * 1024, Compression: pebble.SnappyCompression},  // L1 8MB
			{TargetFileSize: 16 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L2 16MB
			{TargetFileSize: 32 * 1024 * 1024, Compression: pebble.SnappyCompression}, // L3 32MB
			{TargetFileSize: 64 * 1024 * 1024, Compression: pebble.ZstdCompression},   // L4 64MB
			{TargetFileSize: 128 * 1024 * 1024, Compression: pebble.ZstdCompression},  // L5 128MB
			{TargetFileSize: 256 * 1024 * 1024, Compression: pebble.ZstdCompression},  // L6 256MB
		},
	}
}
