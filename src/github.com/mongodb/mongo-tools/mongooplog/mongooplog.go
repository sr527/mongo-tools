package mongooplog

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	commonopts "github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/mongooplog/options"
	"gopkg.in/mgo.v2/bson"
	"strings"
)

type MongoOplog struct {
	// standard tool options
	ToolOptions *commonopts.ToolOptions

	// mongooplog-specific options
	SourceOptions *options.SourceOptions

	// session providers for the two servers used
	// TODO: --dbpath
	SessionProviderFrom *db.SessionProvider
	SessionProviderTo   *db.SessionProvider
}

func (self *MongoOplog) Run() error {

	// split the namespace into database + collection
	dotIndex := strings.Index(self.SourceOptions.OplogNS, ".")

	// TODO: real validation
	if dotIndex == -1 {
		return fmt.Errorf("namespace %v is not valid",
			self.SourceOptions.OplogNS)
	}

	oplogdb := self.SourceOptions.OplogNS[:dotIndex]
	oplogcoll := self.SourceOptions.OplogNS[dotIndex+1:]

	// create a tailing query to the --from server's oplog
	fromSession, err := self.SessionProviderFrom.GetSession()
	if err != nil {
		return fmt.Errorf("error connecting to source db: %v", err)
	}
	defer fromSession.Close()

	// TODO: real wait time, $gte query
	tail := fromSession.DB(oplogdb).C(oplogcoll).Find(bson.M{}).Tail(0)
	defer tail.Close()

	// connect to the destination server
	toSession, err := self.SessionProviderTo.GetSession()
	if err != nil {
		return fmt.Errorf("error connecting to destination db: %v", err)
	}
	defer toSession.Close()

	// grab the dest server's admin db
	adminDB := toSession.DB("admin")

	// read the cursor dry, applying ops to the --host server in the process
	doc := &Oplog{}
	res := map[string]interface{}{}
	for tail.Next(doc) {

		if doc.Operation == "n" {
			continue
		}

		opsToApply := []Oplog{*doc}

		if err := adminDB.Run(bson.M{"applyOps": opsToApply}, &res); err != nil {
			return fmt.Errorf("error applying ops: %v", err)
		}
	}

	return nil
}

// TODO: move this to common / merge with the one in mongodump
type Oplog struct {
	Timestamp bson.MongoTimestamp `bson:"ts"`
	HistoryID int64               `bson:"h"`
	Version   int                 `bson:"v"`
	Operation string              `bson:"op"`
	Namespace string              `bson:"ns"`
	Object    bson.M              `bson:"o"`
	Query     bson.M              `bson:"o2"`
}
