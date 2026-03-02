package lock

import (
	"log"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	lockname string
	LID      string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lid := kvtest.RandValue(12)
	lk := &Lock{ck: ck, lockname: lockname, LID: lid}
	// You may add code here
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.ErrNoKey {
			// lock doesn't exist, try to create it
			err = lk.ck.Put(lk.lockname, lk.LID, 0)
			if err == rpc.OK {
				return
			} else if err == rpc.ErrMaybe {
				// maybe created, check if we got it
				val, version, err = lk.ck.Get(lk.lockname)
				if err == rpc.OK && val == lk.LID {
					return
				} else {
					// another client created the lock, retry
					time.Sleep(100 * time.Millisecond)
					continue
				}
			} else if err == rpc.ErrVersion {
				// another client created the lock, retry
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				log.Fatalln("unexpected error from Put:", err)
			}
		} else if err == rpc.OK || err == rpc.ErrMaybe {
			if val == "" {
				// lock is free, try to acquire it
				err = lk.ck.Put(lk.lockname, lk.LID, version)
				if err == rpc.OK {
					return
				} else if err == rpc.ErrMaybe {
					// maybe created, check if we got it
					val, version, err = lk.ck.Get(lk.lockname)
					if err == rpc.OK && val == lk.LID {
						return
					} else {
						// another client created the lock, retry
						time.Sleep(100 * time.Millisecond)
						continue
					}
				} else if err == rpc.ErrVersion {
					// another client acquired the lock, retry
					time.Sleep(100 * time.Millisecond)
					continue
				} else {
					log.Fatalln("unexpected error from Put:", err)
				}
			}
			if val == lk.LID {
				// lock already held by this client, return
				return
			} else {
				// lock held by another client, wait and retry
				time.Sleep(100 * time.Millisecond)
				continue
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	for {
		val, version, err := lk.ck.Get(lk.lockname)
		if err == rpc.OK {
			if val == lk.LID {
				err = lk.ck.Put(lk.lockname, "", version)
				if err == rpc.OK || err == rpc.ErrMaybe {
					return
				} else {
					time.Sleep(100 * time.Millisecond)
					continue
				}
			} else {
				return
			}
		} else if err == rpc.ErrNoKey {
			return
		}
	}
}
