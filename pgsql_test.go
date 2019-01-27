package pgsql

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var expiry = time.Second * 2
var pgDriver = New("host=192.168.0.110 port=5432 user=postgres password= dbname=gotest sslmode=disable")

func TestPgsqlDriver_Lock(t *testing.T) {
	var counter int32
	var waitGroup = new(sync.WaitGroup)
	var name, value, other = "test.lock.name", "test.lock.value", "test.lock.value.other"

	/* test for competition of lock in multiple goroutine */
	for i := 0; i < 50; i++ {
		waitGroup.Add(1)
		go func() {
			ok, _ := pgDriver.Lock(name, value, expiry)
			if ok {
				atomic.AddInt32(&counter, 1)
			}
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()
	if counter != 1 {
		t.Errorf("unexpected result, expect = 1, but = %d", counter)
	}

	/* test for only assign the same value can unlock the mutex */
	pgDriver.Unlock(name, other)
	ok1, _ := pgDriver.Lock(name, value, expiry)
	pgDriver.Unlock(name, value)
	ok2, _ := pgDriver.Lock(name, value, expiry)
	if ok1 || !ok2 {
		t.Errorf("unexpected result, expect = [false,true] but = [%#v,%#v]", ok1, ok2)
	}

	/* test for the broadcast notification which triggered by unlock */
	commonWatchAndNotifyTest(
		name,
		func() bool {
			ok, _ := pgDriver.Lock(name, value, expiry)
			return ok
		},
		func() {
			pgDriver.Unlock(name, value)
		},
	)
}

func TestRedisDriver_Touch(t *testing.T) {
	var name, value, other = "test.touch.name", "test.touch.value", "test.touch.value.other"
	ok1 := pgDriver.Touch(name, value, expiry)
	pgDriver.Lock(name, value, expiry)
	ok2 := pgDriver.Touch(name, value, expiry)
	ok3 := pgDriver.Touch(name, other, expiry)
	if ok1 || !ok2 || ok3 {
		t.Errorf("unexpected result, expect = [false,true,false] but = [%#v,%#v,%#v]", ok1, ok2, ok3)
	}
}

func TestRedisDriver_RLock(t *testing.T) {
	var counter int32
	var waitGroup = new(sync.WaitGroup)
	var name, value, other = "test.rlock.name", "test.rlock.value", "test.rlock.value.other"

	/* test for read locks are not mutually exclusive */
	for i := 0; i < 10; i++ {
		waitGroup.Add(1)
		go func() {
			for {
				ok, _ := pgDriver.RLock(name, value, expiry)
				if ok {
					atomic.AddInt32(&counter, 1)
					break
				} else {
					time.Sleep(time.Millisecond)
				}
			}
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()
	if counter != 10 {
		t.Errorf("unexpected result, expect = 10, but = %d", counter)
	}

	/* test for write is mutually exclusive with read,
	 * no read lock can enter in after write lock if even it is failed */
	ok1, _ := pgDriver.WLock(name, value, expiry)
	ok2, _ := pgDriver.RLock(name, value, expiry)
	if ok1 || ok2 {
		t.Errorf("unexpected result, expect = [false,false], but = [%#v,%#v]", ok1, ok2)
	}

	/* test for only assign the same value can unlock the mutex
	 * after read unlock, if write lock tried before, it will be preferential */
	for i := 0; i < 10; i++ {
		pgDriver.RUnlock(name, other)
	}
	ok3, _ := pgDriver.WLock(name, value, expiry)
	for i := 0; i < 10; i++ {
		pgDriver.RUnlock(name, value)
	}
	ok4, _ := pgDriver.RLock(name, value, expiry)
	ok5, _ := pgDriver.WLock(name, value, expiry)
	pgDriver.WUnlock(name, value)
	ok6, _ := pgDriver.RLock(name, value, expiry)
	if ok3 || ok4 || !ok5 || !ok6 {
		t.Errorf("unexpected result, expect = [false,false,true,true], but = [%#v,%#v,%#v,%#v]", ok3, ok4, ok5, ok6)
	}

	/* test for the broadcast notification which triggered by unlock */
	commonWatchAndNotifyTest(
		name,
		func() bool {
			ok, _ := pgDriver.RLock(name, value, expiry)
			return ok
		},
		func() {
			pgDriver.RUnlock(name, value)
		},
	)
}

func TestRedisDriver_WLock(t *testing.T) {
	var counter int32
	var waitGroup = new(sync.WaitGroup)
	var name, value, _ = "test.wlock.name", "test.wlock.value", "test.wlock.value.other"

	/* test for write locks are mutually exclusive */
	for i := 0; i < 10; i++ {
		waitGroup.Add(1)
		go func() {
			ok, _ := pgDriver.WLock(name, value, expiry)
			if ok {
				atomic.AddInt32(&counter, 1)
			}
			waitGroup.Done()
		}()
	}
	waitGroup.Wait()
	if counter != 1 {
		t.Errorf("unexpected result, expect = 1, but = %d", counter)
	}
}

func commonWatchAndNotifyTest(name string, lock func() bool, unlock func()) {
	unlock()
	var waitGroup sync.WaitGroup
	for i := 0; i < 5; i++ {
		waitGroup.Add(1)
		go func() {
			msgCounter := 0
			notifyChan := pgDriver.Watch(name)
			for {
				select {
				case <-notifyChan:
					msgCounter++
					if msgCounter == 50 {
						waitGroup.Done()
					}
				}
			}
		}()
	}
	waitGroup.Add(1)
	go func() {
		/* make ensure all channels are ready */
		time.Sleep(time.Millisecond * 1200)
		for i := 1; i <= 50; i++ {
			if lock() {
				unlock()
			}
		}
		waitGroup.Done()
	}()
	waitGroup.Wait()
}
