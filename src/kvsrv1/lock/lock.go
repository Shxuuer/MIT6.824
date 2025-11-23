package lock

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here
	Key         string
	LockVersion rpc.Tversion
	ID          string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	lk := &Lock{ck: ck, Key: l, LockVersion: 0, ID: kvtest.RandValue(8)}
	// You may add code here
	for {
		err := lk.ck.Put(lk.Key, "", 0)
		if err == rpc.OK || err == rpc.ErrVersion {
			break
		}
	}
	return lk
}

func (lk *Lock) Acquire() {
	// Your code here
	for {
		val, version, _ := lk.ck.Get(lk.Key)
		if val != "" {
			continue
		}
		err := lk.ck.Put(lk.Key, lk.ID, version)
		if err == rpc.OK {
			lk.LockVersion = version + 1
			break
		}
		if err == rpc.ErrVersion {
			continue
		}
		if err == rpc.ErrMaybe {
			val, version_, _ := lk.ck.Get(lk.Key)
			if version_ == version+1 && val == lk.ID {
				lk.LockVersion = version + 1
				break
			}
		}
	}
}

func (lk *Lock) Release() {
	// Your code here
	lk.ck.Put(lk.Key, "", lk.LockVersion)
}
