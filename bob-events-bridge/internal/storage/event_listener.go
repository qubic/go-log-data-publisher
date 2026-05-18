package storage

import (
	"log"

	"github.com/cockroachdb/pebble"
)

type PebbleEventListener struct {
	pebble.EventListener

	epoch uint32
}

func NewPebbleEventListener(epoch uint32) *PebbleEventListener {
	l := &PebbleEventListener{epoch: epoch}
	l.BackgroundError = l.backgroundError
	l.CompactionBegin = l.compactionBegin
	l.CompactionEnd = l.compactionEnd
	l.FlushBegin = l.flushBegin
	l.FlushEnd = l.flushEnd
	l.WriteStallBegin = l.writeStallBegin
	l.WriteStallEnd = l.writeStallEnd
	return l
}

func (l *PebbleEventListener) backgroundError(err error) {

	log.Printf("[PEBBLE-EP%d]: Encountered background error: %v", l.epoch, err)

}

func (l *PebbleEventListener) compactionBegin(info pebble.CompactionInfo) {

	log.Printf("[PEBBLE-EP%d]: Compaction triggered. JobID: %d. Reason: %s. ", l.epoch, info.JobID, info.Reason)
	for _, level := range info.Input {
		log.Printf("  From Level %d - %s", level.Level, level.String())
	}
	log.Printf("  To level %d %ss", info.Output.Level, info.Output.String())

}

func (l *PebbleEventListener) compactionEnd(info pebble.CompactionInfo) {
	log.Printf("[PEBBLE-EP%d]: Compaction with JobID %d ended. Took %v.", l.epoch, info.JobID, info.TotalDuration)
}

func (l *PebbleEventListener) flushBegin(info pebble.FlushInfo) {
	log.Printf("[PEBBLE-EP%d]: Flush triggered. JobID: %d. Reason: %s.", l.epoch, info.JobID, info.Reason)
}

func (l *PebbleEventListener) flushEnd(info pebble.FlushInfo) {
	log.Printf("[PEBBLE-EP%d]: Flush with JobID %d ended. Took %v.", l.epoch, info.JobID, info.TotalDuration)
}

func (l *PebbleEventListener) writeStallBegin(info pebble.WriteStallBeginInfo) {
	log.Printf("[PEBBLE-EP%d]: Writes stalled. Reason: %s.", l.epoch, info.Reason)
}

func (l *PebbleEventListener) writeStallEnd() {
	log.Printf("[PEBBLE-EP%d]: Writes resumed.", l.epoch)
}
