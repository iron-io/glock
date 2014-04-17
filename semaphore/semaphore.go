package semaphore

import (
	"sync"
	"sync/atomic"
	"time"
)

type Empty struct {
}

type Glock struct {
	Locks     map[string]*Lock
	LocksLock sync.RWMutex
}

func NewGlock() *Glock {
	return &Glock{make(map[string]*Lock), sync.RWMutex{}}
}

func (g *Glock) GetOrCreateLock(key string, size int64) *Lock {
	if l, ok := g.GetLock(key); !ok {
		// lock not found
		// create it
		return g.CreateLock(key, size)
	} else {
		// resize lock and return
		l.Resize(size)
		return l
	}
}

// lock and check if a matching lock exists
// if it does return it
// if not create a new lock
// note: matching lock requires the same key and the same size; if size is different, it will be
// replaced
func (g *Glock) GetLock(key string) (*Lock, bool) {
	g.LocksLock.RLock()
	l, ok := g.Locks[key]
	g.LocksLock.RUnlock()
	// in most cases a lock already exists so we use a read lock
	// to confirm that
	return l, ok
}

func (g *Glock) CreateLock(key string, size int64) *Lock {
	// let's acquire write lock and check again and see if someone else created one already
	g.LocksLock.Lock()
	if l, ok := g.Locks[key]; ok {
		// someone created one in the meantime, let's return that
		l.Resize(size)
		g.LocksLock.Unlock()
		return l
	}
	// lock still does not exist; let's create one
	l := &Lock{Semaphore: int64(0), SemaphoreLock: sync.Mutex{}, Capacity: size, TimeoutSet: make(map[int64]bool), Id: int64(0), TimeoutSetLock: sync.Mutex{}}
	l.Cond = sync.NewCond(&l.SemaphoreLock)
	g.Locks[key] = l
	g.LocksLock.Unlock()
	return l
}

// note: no overflow on int64 - we will probably never get to the end of int64
type Lock struct {
	Semaphore      int64
	SemaphoreLock  sync.Mutex
	Capacity       int64
	TimeoutSet     map[int64]bool
	Id             int64
	TimeoutSetLock sync.Mutex
	Cond           *sync.Cond
}

// Blocking lock
func (l *Lock) BLock(timeout int) int64 {
	var lockId int64
	l.SemaphoreLock.Lock()
	for {
		if l.Semaphore >= l.Capacity {
			l.Cond.Wait()
		} else {
			break
		}
	}
	l.Semaphore++
	lockId = l.lock(timeout)
	l.SemaphoreLock.Unlock()

	return lockId
}

// Non Blocking lock
func (l *Lock) NLock(timeout int) int64 {
	l.SemaphoreLock.Lock()
	if l.Semaphore < l.Capacity {
		l.Semaphore++
		l.SemaphoreLock.Unlock()
		return l.lock(timeout)
	}

	l.SemaphoreLock.Unlock()
	return int64(0)
}

// increment id
func (l *Lock) lock(timeout int) int64 {
	id := atomic.AddInt64(&l.Id, 1)

	l.TimeoutSetLock.Lock()
	// increment the id each time
	// impossible to get a collision
	// so we don't need to check whether key exists
	l.TimeoutSet[id] = true
	l.TimeoutSetLock.Unlock()

	time.AfterFunc(time.Duration(timeout)*time.Millisecond, func() {
		l.Unlock(id)
	})
	return id
}

func (l *Lock) Unlock(id int64) bool {
	l.TimeoutSetLock.Lock()
	n := len(l.TimeoutSet)
	delete(l.TimeoutSet, id)
	deleted := n != len(l.TimeoutSet)
	l.TimeoutSetLock.Unlock()

	if deleted {
		l.SemaphoreLock.Lock()
		if l.Semaphore > 0 {
			l.Semaphore--
		}
		count := l.Semaphore
		if count == l.Capacity-1 {
			l.SemaphoreLock.Unlock()
			l.Cond.Broadcast()
			return true
		}
		l.SemaphoreLock.Unlock()
	}
	return false
}

func (l *Lock) Resize(size int64) {
	l.SemaphoreLock.Lock()
	if l.Capacity != size {
		l.Capacity = size
	}
	l.SemaphoreLock.Unlock()
}
