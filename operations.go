package mdb

import (
	"encoding/json"
	"errors"

	"github.com/Sirupsen/logrus"
	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

var (
	NoQuery    error = errors.New("No query specified")
	NotChanged error = errors.New("Nothing fits query")
	NotFound   error = mgo.ErrNotFound
	IsDup      error = errors.New("Is duplicate")
)

type CallbackInterface interface {
	Do(bson.M) error
}

func (this Collection) Create(query interface{}) error {
	session := this.coll.Database.Session.Copy()
	defer session.Close()
	coll := session.DB(this.coll.Database.Name).C(this.coll.Name)
	err := coll.Insert(query)
	if this.logger != nil {
		this.logger.WithFields(logrus.Fields{
			"query": loggerJSON(query),
			"error": err,
		}).Debug("Create")
	}

	if mgo.IsDup(err) {
		return IsDup
	}

	return err
}

func (this Collection) Get(options Opt, result interface{}) (err error) {
	session := this.coll.Database.Session.Copy()
	defer session.Close()
	coll := session.DB(this.coll.Database.Name).C(this.coll.Name)

	if options.Find == nil {
		return NoQuery
	}

	query := coll.Find(options.Find)
	query = addSorting(query, options)
	query = addPagination(query, options)
	err = multi(query, options)(result)

	if this.logger != nil {
		options.Print(this.logger, "get").Debug("Get")
	}

	return err

}

func (this *Collection) Aggregate(options Opt, result interface{}) (err error) {
	session := this.coll.Database.Session.Copy()
	defer session.Close()
	coll := session.DB(this.coll.Database.Name).C(this.coll.Name)

	if options.Find == nil {
		return NoQuery
	}

	query := coll.Pipe(options.Find)
	err = multi(query, options)(result)

	if this.logger != nil {
		options.Print(this.logger, "aggregate").Debug("Aggregate")
	}

	return err
}

func (this *Collection) Update(options Opt) (err error) {
	session := this.coll.Database.Session.Copy()
	defer session.Close()
	coll := session.DB(this.coll.Database.Name).C(this.coll.Name)

	if options.Find == nil || options.Update == nil {
		return NoQuery
	}

	if options.Upsert {
		if info, err := coll.Upsert(options.Find, options.Update); info.Updated == 0 && err == nil {
			err = NotChanged
		}
	} else {
		if options.Multi {
			if info, err := coll.UpdateAll(options.Find, options.Update); info.Updated == 0 && err == nil {
				err = NotChanged
			}
		} else {
			err = coll.Update(options.Find, options.Update)
		}

	}

	if this.logger != nil {
		options.Print(this.logger, "update").Debug("Update")
	}

	return err
}

// func (this *Collection) FindAndModify(search, sel, update interface{}, isNew bool, res interface{}) error {
// 	session := this.coll.Database.Session.Copy()
// 	defer session.Close()
// 	coll := session.DB(this.coll.Database.Name).C(this.coll.Name)

// 	_, err := coll.Find(search).Apply(mgo.Change{Update: update, ReturnNew: isNew}, res)
// 	if this.logger != nil {
// 		this.logger.WithFields(logrus.Fields{
// 			"query":  loggerJSON(search),
// 			"update": loggerJSON(update),
// 			"select": loggerJSON(sel),
// 			"error":  err,
// 		}).Debug("Find and modify")
// 	}
// 	return err
// }

func (this Collection) Remove(options Opt) (err error) {
	session := this.coll.Database.Session.Copy()
	defer session.Close()
	coll := session.DB(this.coll.Database.Name).C(this.coll.Name)

	if options.Multi {
		if info, err := coll.RemoveAll(options.Find); info.Removed == 0 && err == nil {
			err = NotChanged
		}
	} else {
		err = coll.Remove(options.Find)
	}

	if this.logger != nil {
		options.Print(this.logger, "remove").Debug("Remove")
	}
	return err
}

// func (this *Collection) Count(query interface{}) (int, error) {
// 	session := this.coll.Database.Session.Copy()
// 	defer session.Close()
// 	coll := session.DB(this.coll.Database.Name).C(this.coll.Name)
// 	n, err := coll.Find(query).Count()
// 	if this.logger != nil {
// 		this.logger.WithFields(logrus.Fields{
// 			"query": loggerJSON(query),
// 			"error": err,
// 		}).Debug("Count")
// 	}
// 	return n, err
// }

// func (this *Collection) Iterate(query interface{}, sel interface{}, callback CallbackInterface) error {
// 	session := this.coll.Database.Session.Copy()
// 	defer session.Close()
// 	coll := session.DB(this.coll.Database.Name).C(this.coll.Name)

// 	iter := coll.Find(query).Select(sel).Iter()
// 	res := bson.M{}

// 	for iter.Next(&res) {
// 		if err := callback.Do(res); err != nil {
// 			if this.logger != nil {
// 				this.logger.WithFields(logrus.Fields{
// 					"query": loggerJSON(query),
// 					"sel":   loggerJSON(sel),
// 					"error": err,
// 				}).Error("Next iteration")
// 			}
// 			return err
// 		}

// 	}

// 	err := iter.Close()

// 	if this.logger != nil {
// 		this.logger.WithFields(logrus.Fields{
// 			"query": loggerJSON(query),
// 			"sel":   loggerJSON(sel),
// 			"error": err,
// 		}).Debug("Iterate")
// 	}

// 	return err
// }

// // handleNotFoundErrorDB tells when error occured because there's no such entry ot user just doesn't have access to it.
// func (this *DB) handleNotFoundError(c echo.Context, err error, collection string, search bson.M, outErr Error) error {
// 	if isNotFound(err) {
// 		// Check that data exists. If so, tell that user can't have access to it.
// 		if err := this.Collections[collection].Get(search, nil, nil); err == nil {
// 			return c.JSON(http.StatusForbidden, Error{ErrDenied, "Can't change this data"})
// 		} else {
// 			return c.JSON(http.StatusBadRequest, outErr)
// 		}
// 	} else {
// 		return c.JSON(http.StatusInternalServerError, InternalError)
// 	}
// }

func loggerJSON(msg interface{}) interface{} {
	res, err := json.MarshalIndent(msg, "", "   ")
	if err != nil {
		return msg
	} else {
		return string(res)
	}
}

// // IfInArrayProjection return query which tells if value in array field.
// func IfInArrayProjection(value string, field string) bson.M {
// 	return bson.M{
// 		"$cond": []interface{}{
// 			bson.M{
// 				"$eq": []interface{}{
// 					bson.M{
// 						"$size": bson.M{
// 							"$setIntersection": []interface{}{
// 								[]interface{}{value},
// 								"$" + field,
// 							},
// 						},
// 					}, 1,
// 				},
// 			},
// 			true,
// 			false,
// 		},
// 		// "$cond": []interface{}{
// 		// 	bson.M{
// 		// 		"$eq": []interface{}{
// 		// 			"$" + field,
// 		// 			value,
// 		// 		},
// 		// 	},
// 		// 	true,
// 		// 	false,
// 		// },
// 	}
// }
