package fsm

import (
	"fmt"
	"log"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

type seshatSnapshot struct {
	Conn *badger.DB
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (snap *seshatSnapshot) Persist(sink raft.SnapshotSink) error {
	log.Println("Persisting snapshot")
	_, err := snap.Conn.Backup(sink, 0)
	if err != nil {
		return fmt.Errorf("error persisting snapshot: %s", err)
	}

	err = sink.Close()
	if err != nil {
		return fmt.Errorf("error closing snapshot: %s", err)
	}
	return nil
}

// Release is invoked when we are finished with the snapshot.
func (snap *seshatSnapshot) Release() {
	log.Println("Releasing snapshot")
}
