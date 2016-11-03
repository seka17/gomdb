package mdb

import (
	"strings"

	"github.com/Sirupsen/logrus"

	mgo "gopkg.in/mgo.v2"
)

type multiInterface interface {
	All(interface{}) error
	One(interface{}) error
}

type Opt struct {
	Find   interface{}
	Update interface{}
	Select interface{}
	Skip   int
	Limit  int
	Sort   string
	DoSort bool
	Upsert bool
	Multi  bool
}

func (this Opt) FormSortQuery() []string {
	if this.DoSort {
		if this.Sort == "" {
			return []string{"_id"}
		} else {
			return strings.Split(this.Sort, ",")
		}
	}
	return nil
}

func addSorting(query *mgo.Query, opt Opt) *mgo.Query {
	if v := opt.FormSortQuery(); v != nil {
		return query.Sort(v...)
	}
	return query
}

func addPagination(query *mgo.Query, opt Opt) *mgo.Query {
	return query.Select(opt.Select).Limit(opt.Limit).Skip(opt.Skip)
}

func multi(query multiInterface, opt Opt) func(interface{}) error {
	if opt.Multi {
		return query.All
	} else {
		return query.One
	}
}

func (this Opt) Print(logger *logrus.Entry, queryType string) *logrus.Entry {
	if logger == nil {
		return nil
	}

	msg := logrus.Fields{
		"find":  loggerJSON(this.Find),
		"multi": this.Multi,
	}

	switch queryType {
	case "get":
		if this.Skip != 0 {
			msg["skip"] = this.Skip
		}

		if this.Limit != 0 {
			msg["limit"] = this.Limit
		}

		if this.Select != nil {
			msg["select"] = loggerJSON(this.Select)
		}

		if this.DoSort {
			if this.Sort == "" {
				msg["sort"] = "_id"
			} else {
				msg["sort"] = this.Sort
			}
		}
		break
	case "remove", "aggregate":
		break
	case "update":
		msg["upsert"] = this.Upsert
		if this.Update != nil {
			msg["update"] = loggerJSON(this.Update)
		}
		break
	}

	return logger.WithFields(msg)
}
