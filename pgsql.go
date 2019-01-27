package pgsql

import (
	"database/sql"
	"time"

	"github.com/go-locks/distlock/driver"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
)

var (
	MaxReaders            = 1 << 30
	MinWatchRetryInterval = time.Millisecond
	MaxWatchRetryInterval = time.Second * 16
)

type pgsqlDriver struct {
	dsn    []string
	dbs    []*sql.DB
	quorum int
}

var _ driver.IWatcher = &pgsqlDriver{}
var _ driver.IDriver = &pgsqlDriver{}
var _ driver.IRWDriver = &pgsqlDriver{}

func New(dsn ...string) *pgsqlDriver {
	dbs := make([]*sql.DB, len(dsn))
	for i, d := range dsn {
		if db, err := sql.Open("postgres", d); err != nil {
			// normally, it won't happen
			logrus.WithError(err).Panicf("open pgsql for distlock failed")
		} else {
			dbs[i] = db
		}
	}
	return &pgsqlDriver{
		dsn:    dsn,
		dbs:    dbs,
		quorum: len(dbs),
	}
}

func (pd *pgsqlDriver) channelName(name string) string {
	return "unlock-notify-channel-{" + name + "}"
}

func (pd *pgsqlDriver) doLock(fn func(db *sql.DB) int) (bool, time.Duration) {
	counter, minWait := 0, -1
	for _, db := range pd.dbs {
		if wait := fn(db); wait == -3 {
			counter++
		} else if minWait > wait || minWait == -1 {
			minWait = wait
		}
	}
	var w time.Duration
	if minWait > 0 {
		w = time.Duration(minWait) * time.Millisecond
	} else {
		w = time.Duration(minWait) // less than zero, use the default wait duration
	}
	return counter >= pd.quorum, w
}

func (pd *pgsqlDriver) doTouch(fn func(db *sql.DB) bool) bool {
	var counter int
	for _, db := range pd.dbs {
		if fn(db) {
			counter++
		}
	}
	return counter >= pd.quorum
}

func (pd *pgsqlDriver) Lock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doLock(func(db *sql.DB) (wait int) {
		err := db.QueryRow("SELECT distlock.lock($1, $2, $3);", name, value, msExpiry).Scan(&wait)
		if err != nil {
			wait = -1 // less than zero, use the default wait duration
			logrus.WithError(err).Errorf("pgsql acquire lock '%s' failed", name)
		}
		return
	})
}

func (pd *pgsqlDriver) Unlock(name, value string) {
	channel := pd.channelName(name)
	for _, db := range pd.dbs {
		var str string
		err := db.QueryRow("SELECT distlock.unlock($1, $2, $3);", name, value, channel).Scan(&str)
		if err != nil {
			logrus.WithError(err).Errorf("pgsql release lock '%s' failed", name)
		}
	}
}

func (pd *pgsqlDriver) Touch(name, value string, expiry time.Duration) (ok bool) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doTouch(func(db *sql.DB) (ok bool) {
		err := db.QueryRow("SELECT distlock.touch($1, $2, $3);", name, value, msExpiry).Scan(&ok)
		if err != nil {
			logrus.WithError(err).Errorf("pgsql touch lock '%s' failed", name)
		}
		return
	})
}

func (pd *pgsqlDriver) RLock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doLock(func(db *sql.DB) (wait int) {
		err := db.QueryRow("SELECT distlock.rlock($1, $2, $3);", name, value, msExpiry).Scan(&wait)
		if err != nil {
			wait = -1 // less than zero, use the default wait duration
			logrus.WithError(err).Errorf("pgsql acquire read lock '%s' failed", name)
		}
		return
	})
}

func (pd *pgsqlDriver) RUnlock(name, value string) {
	channel := pd.channelName(name)
	for _, db := range pd.dbs {
		var str string
		err := db.QueryRow("SELECT distlock.runlock($1, $2, $3, $4);", name, value, channel, MaxReaders).Scan(&str)
		if err != nil {
			logrus.WithError(err).Errorf("pgsql release read lock '%s' failed", name)
		}
	}
}

func (pd *pgsqlDriver) RTouch(name, value string, expiry time.Duration) (ok bool) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doTouch(func(db *sql.DB) (ok bool) {
		err := db.QueryRow("SELECT distlock.rwtouch($1, $2, $3);", name, value, msExpiry).Scan(&ok)
		if err != nil {
			logrus.WithError(err).Errorf("pgsql touch rw lock '%s' failed", name)
		}
		return
	})
}

func (pd *pgsqlDriver) WLock(name, value string, expiry time.Duration) (ok bool, wait time.Duration) {
	msExpiry := int(expiry / time.Millisecond)
	return pd.doLock(func(db *sql.DB) (wait int) {
		err := db.QueryRow("SELECT distlock.wlock($1, $2, $3, $4);", name, value, msExpiry, MaxReaders).Scan(&wait)
		if err != nil {
			wait = -1 // less than zero, use the default wait duration
			logrus.WithError(err).Errorf("pgsql acquire write lock '%s' failed", name)
		}
		return
	})
}

func (pd *pgsqlDriver) WUnlock(name, value string) {
	channel := pd.channelName(name)
	for _, db := range pd.dbs {
		var str string
		err := db.QueryRow("SELECT distlock.wunlock($1, $2, $3, $4);", name, value, channel, MaxReaders).Scan(&str)
		if err != nil {
			logrus.WithError(err).Errorf("pgsql release write lock '%s' failed", name)
		}
	}
}

func (pd *pgsqlDriver) WTouch(name, value string, expiry time.Duration) (ok bool) {
	return pd.RTouch(name, value, expiry)
}

func (pd *pgsqlDriver) Watch(name string) <-chan struct{} {
	channel := pd.channelName(name)
	outChan := make(chan struct{})
	for _, dsn := range pd.dsn {
		go func() {
			listener := pq.NewListener(dsn, MinWatchRetryInterval, MaxWatchRetryInterval, func(event pq.ListenerEventType, err error) {
				if err != nil {
					logrus.WithError(err).Errorf("pgsql listen channel '%s' error", channel)
				}
			})
			if err := listener.Listen(channel); err != nil {
				// normally, it won't happen
				logrus.WithError(err).Panicf("pgsql listen channel '%s' error", channel)
			}
			for {
				<-listener.Notify
				outChan <- struct{}{}
			}
		}()
	}
	return outChan
}
