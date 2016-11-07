package mdb

import (
	"testing"

	"github.com/Sirupsen/logrus"
)

type TestOperations struct {
	Opt     Opt
	IsError bool
}

func TestGet(t *testing.T) {
	logger := logrus.StandardLogger()
	db, err := InitDB("localhost:27017/test", logger)
	if err != nil {
		t.FailNow()
	}

	if err := db.AddCollection("test", nil); err != nil {
		t.FailNow()
	}

	tests := []TestOperations{
		TestOperations{
			Opt:     Opt{Find: M{"garbage": "lol"}},
			IsError: true,
		},
	}

	for _, test := range tests {
		if err := db.MustCollection("test").Get(test.Opt, nil); (err != nil) != test.IsError {
			logger.WithField("opt", test.Opt).Error("Fail")
			t.FailNow()
		}
	}
}
