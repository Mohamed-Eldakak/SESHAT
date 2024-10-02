package fsm

import (
	"io"

	"Seshat/utils"

	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
)

type seshatFSM struct {
	Conn *badger.DB
}

// type LogStruct struct {
// 	Op  string
// 	Key []byte
// 	Val []byte
// }

func NewseshatFSM(path string) (*seshatFSM, error) {
	var err error
	opts := badger.DefaultOptions(path)
	opts.Logger = nil
	opts.SyncWrites = true

	handle, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}

	return &seshatFSM{
		Conn: handle,
	}, nil
}

// Apply log is invoked once a log entry is committed.
// It returns a value which will be made available in the
// ApplyFuture returned by Raft.Apply method if that
// method was called on the same Raft node as the FSM.
func (fsm *seshatFSM) Apply(log *raft.Log) interface{} {
	// var data CommandPayload
	// var err error

	// err = decodeMsgPack(log.Data, &data)
	// if err != nil {
	// 	return err
	// }
	// if data.Operation == "set" {
	// 	err = fsm.Conn.Update(func(txn *badger.Txn) error {
	// 		return txn.Set(data.Key, data.Value)
	// 	})
	// } else if data.Operation == "delete" {
	// 	err = fsm.Conn.Update(func(txn *badger.Txn) error {
	// 		return txn.Delete(data.Key)
	// 	})
	// }
	// if err != nil {
	// 	return err
	// }
	// return nil
	if log.Type == raft.LogCommand {
		var payload CommandPayload
		if err := utils.DecodeMsgPack(log.Data, &payload); err != nil {
			return err
		}
		if payload.Operation == "set" {
			return &ApplyResponse{
				Error: fsm.Conn.Update(func(txn *badger.Txn) error {
					return txn.Set(payload.Key, payload.Value)
				}),
				Data: payload.Value,
			}
		} else if payload.Operation == "delete" {
			return &ApplyResponse{
				Error: fsm.Conn.Update(func(txn *badger.Txn) error {
					return txn.Delete(payload.Key)
				}),
				Data: nil,
			}
		} else if payload.Operation == "get" {
			data, err := fsm.Get(payload.Key)
			if err != nil {
				return &ApplyResponse{
					Error: err,
					Data:  data,
				}
			}
		}
	}

	return nil
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time snapshot of the FSM.
func (fsm *seshatFSM) Snapshot() (raft.FSMSnapshot, error) {
	return &seshatSnapshot{Conn: fsm.Conn}, nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (fsm *seshatFSM) Restore(r io.ReadCloser) error {
	err := fsm.Conn.DropAll()
	if err != nil {
		return err
	}
	err = fsm.Conn.Load(r, 100)
	if err != nil {
		return err
	}
	return nil
}

func (fsm *seshatFSM) Get(key []byte) ([]byte, error) {
	var val []byte
	err := fsm.Conn.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
		}
		val, err = item.ValueCopy(val)
		return err
	})
	if err != nil {
		return nil, err
	}
	return val, nil
}
