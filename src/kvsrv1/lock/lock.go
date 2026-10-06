package lock

import (
	"strconv"
	"strings"
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
)

// leaseDuration bounds how long a holder may keep the lock without
// renewing it; an expired lease can be stolen by another client, which
// recovers the lock if its holder crashed.
const leaseDuration = 3 * time.Second

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck       kvtest.IKVClerk
	lockname string
	clientID string

	mu     sync.Mutex
	stopCh chan struct{} // closed by Release to stop the renewal goroutine
	doneCh chan struct{} // closed by the renewal goroutine once it has exited
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, clientID: kvtest.RandValue(8)}
	return lk
}

func encodeHolder(id string, expiry time.Time) string {
	return id + "|" + strconv.FormatInt(expiry.UnixNano(), 10)
}

func decodeHolder(value string) (id string, expiry time.Time, ok bool) {
	i := strings.LastIndex(value, "|")
	if i < 0 {
		return "", time.Time{}, false
	}
	ns, err := strconv.ParseInt(value[i+1:], 10, 64)
	if err != nil {
		return "", time.Time{}, false
	}
	return value[:i], time.Unix(0, ns), true
}

func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.lockname)

		switch err {
		case rpc.ErrNoKey:
			newVal := encodeHolder(lk.clientID, time.Now().Add(leaseDuration))
			putErr := lk.ck.Put(lk.lockname, newVal, 0)
			if putErr == rpc.OK {
				lk.startRenewal()
				return
			}
		case rpc.OK:
			id, expiry, ok := decodeHolder(value)
			if ok && id == lk.clientID {
				lk.startRenewal()
				return
			}
			if value == "unlocked" || (ok && time.Now().After(expiry)) {
				newVal := encodeHolder(lk.clientID, time.Now().Add(leaseDuration))
				putErr := lk.ck.Put(lk.lockname, newVal, version)
				if putErr == rpc.OK {
					lk.startRenewal()
					return
				}
			}
		}

		time.Sleep(50 * time.Millisecond)
	}
}

func (lk *Lock) Release() {
	lk.stopRenewal()

	for {
		value, version, err := lk.ck.Get(lk.lockname)
		if err != rpc.OK {
			return
		}
		id, _, ok := decodeHolder(value)
		if !ok || id != lk.clientID {
			return
		}
		putErr := lk.ck.Put(lk.lockname, "unlocked", version)
		if putErr == rpc.OK {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (lk *Lock) startRenewal() {
	stopCh := make(chan struct{})
	doneCh := make(chan struct{})

	lk.mu.Lock()
	lk.stopCh = stopCh
	lk.doneCh = doneCh
	lk.mu.Unlock()

	go func() {
		defer close(doneCh)
		ticker := time.NewTicker(leaseDuration / 2)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				lk.renew()
			}
		}
	}()
}

func (lk *Lock) renew() {
	value, version, err := lk.ck.Get(lk.lockname)
	if err != rpc.OK {
		return
	}
	id, _, ok := decodeHolder(value)
	if !ok || id != lk.clientID {
		return
	}
	newVal := encodeHolder(lk.clientID, time.Now().Add(leaseDuration))
	lk.ck.Put(lk.lockname, newVal, version)
}

func (lk *Lock) stopRenewal() {
	lk.mu.Lock()
	stopCh := lk.stopCh
	doneCh := lk.doneCh
	lk.stopCh = nil
	lk.doneCh = nil
	lk.mu.Unlock()

	if stopCh != nil {
		close(stopCh)
		<-doneCh
	}
}
