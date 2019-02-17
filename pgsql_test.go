package pgsql

import (
	"testing"

	"github.com/go-locks/testcase"
)

var pgDriver = New("host=192.168.0.110 port=5432 user=postgres password= dbname=filesystem sslmode=disable")

func TestPgsqlDriver_Lock(t *testing.T) {
	testcase.RunLockTest(pgDriver, t)
}

func TestPgsqlDriver_Touch(t *testing.T) {
	testcase.RunTouchTest(pgDriver, t)
}

func TestPgsqlDriver_RLock(t *testing.T) {
	testcase.RunRLockTest(pgDriver, t)
}

func TestPgsqlDriver_RTouch(t *testing.T) {
	testcase.RunRTouchTest(pgDriver, t)
}

func TestPgsqlDriver_WLock(t *testing.T) {
	testcase.RunWLockTest(pgDriver, t)
}

func TestPgsqlDriver_WTouch(t *testing.T) {
	testcase.RunWTouchTest(pgDriver, t)
}
