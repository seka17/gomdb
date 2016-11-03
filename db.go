package mdb

import (
	"errors"

	"github.com/Sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
)

type DB struct {
	session     *mgo.Session
	collections map[string]*Collection
	database    string // name of database in Mongo
	logger      *logrus.Entry
}

type Collection struct {
	coll   *mgo.Collection
	logger *logrus.Entry
}

func InitDB(address string, logger *logrus.Logger) (*DB, error) {
	if address == "" {
		address = "mongodb://localhost/"
	}
	di, err := mgo.ParseURL(address) // parse connection string
	if err != nil {
		return nil, err
	}
	if di.Database == "" {
		di.Database = "test"
	}
	session, err := mgo.DialWithInfo(di) // start connection
	if err != nil {
		return nil, err
	}

	db := &DB{
		session:     session,
		database:    di.Database,
		collections: make(map[string]*Collection),
	}

	if logger != nil {
		db.logger = logger.WithFields(logrus.Fields{
			"db":      di.Database,
			"address": address,
		})
		db.logger.Info("Database initiated")
	}

	return db, nil
}

func (this *DB) AddCollection(name string, indexes []mgo.Index) error {
	coll := this.session.DB(this.database).C(name)
	for _, index := range indexes {
		if err := coll.EnsureIndex(index); err != nil {
			return errors.New(name + ": " + err.Error())
		}
	}

	this.collections[name] = &Collection{
		coll: coll,
	}
	if this.logger != nil {
		this.collections[name].logger = this.logger.WithField("collection", name)
		this.collections[name].logger.Info("Collection added to database")
	}

	return nil
}

func (this DB) Collection(name string) (*Collection, error) {
	if collection, ok := this.collections[name]; ok {
		return collection, nil
	} else {
		return nil, errors.New("There's no " + name + " collection")
	}
}

func (this DB) MustCollection(name string) *Collection {
	return this.collections[name]
}
