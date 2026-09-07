package storage

import (
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/sstable/block"
	"runtime"
)

const size64mb = 67108864

func tunedPebbleOptions(epoch uint32) *pebble.Options {
	options := pebble.DefaultOptions()
	options.WithFSDefaults()
	options.MemTableSize = size64mb
	options.LBaseMaxBytes = size64mb * 8 // 512 MB
	options.TargetFileSizes = [7]int64{
		size64mb,
		size64mb * 2,  // 128 MB
		size64mb * 4,  // 256 MB
		size64mb * 8,  // 512 MB
		size64mb * 16, // 1 GB
		size64mb * 32, // 2 GB
		size64mb * 64, // 4 GB
	}
	options.ApplyCompressionSettings(func() pebble.DBCompressionSettings {
		cs := pebble.DBCompressionSettings{Name: "QubicEventsData"}
		cs.Levels[0] = block.NoCompression
		cs.Levels[1] = block.FastestCompression
		cs.Levels[2] = block.FastestCompression
		cs.Levels[3] = block.FastestCompression
		cs.Levels[4] = block.FastestCompression
		cs.Levels[5] = block.FastCompression
		cs.Levels[6] = block.BalancedCompression
		return cs
	})
	options.CompactionConcurrencyRange = func() (lower, upper int) {
		return 1, max(1, runtime.NumCPU()-1)
	}
	options.AddEventListener(NewPebbleEventListener(epoch).EventListener)

	return options
}
