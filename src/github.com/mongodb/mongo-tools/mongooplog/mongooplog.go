package mongooplog

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	commonopts "github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongooplog/options"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"time"
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

	// split up the oplog namespace we are using
	oplogDB, oplogColl, err :=
		util.SplitAndValidateNamespace(self.SourceOptions.OplogNS)

	if err != nil {
		return err
	}

	// the full oplog namespace needs to be specified
	if oplogColl == "" {
		return fmt.Errorf("the oplog namespace must specify a collection")
	}

	// connect to the source server
	fromSession, err := self.SessionProviderFrom.GetSession()
	if err != nil {
		return fmt.Errorf("error connecting to source db: %v", err)
	}
	defer fromSession.Close()

	// get the tailing cursor for the source server's oplog
	tail := buildTailingCursor(fromSession.DB(oplogDB).C(oplogColl),
		self.SourceOptions)
	defer tail.Close()

	// connect to the destination server
	toSession, err := self.SessionProviderTo.GetSession()
	if err != nil {
		return fmt.Errorf("error connecting to destination db: %v", err)
	}
	defer toSession.Close()

	// applyOps needs to be run on the destination server's admin db
	adminDB := toSession.DB("admin")

	// read the cursor dry, applying ops to the destination
	// server in the process
	oplogEntry := &Oplog{}
	res := &ApplyOpsResponse{}
	for tail.Next(oplogEntry) {

		// make sure there was no tailing error
		if err := tail.Err(); err != nil {
			return fmt.Errorf("error querying oplog: %v", err)
		}

		// skip noops
		if oplogEntry.Operation == "n" {
			continue
		}

		oplogEntry.Operation = "blech"

		// prepare the op to be applied
		opsToApply := []Oplog{*oplogEntry}

		// apply the operation
		err := adminDB.Run(bson.M{"applyOps": opsToApply}, res)

		if err != nil {
			return fmt.Errorf("error applying ops: %v", err)
		}

		// check the server's return for an issue
		if !res.Ok {
			return fmt.Errorf("error applying op: %v", res.ErrMsg)
		}
	}

	return nil
}

// TODO: move this to common
type ApplyOpsResponse struct {
	Ok     bool   `bson:"ok"`
	ErrMsg string `bson:"errmsg"`
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

// get the tailing cursor for the oplog collection, based on the options
// passed in to mongooplog
func buildTailingCursor(oplog *mgo.Collection,
	sourceOptions *options.SourceOptions) *mgo.Iter {

	// get the correct timestamp for the $gte query
	gteTime := time.Now().
		Add(-(time.Duration(sourceOptions.Seconds) * time.Second))
	// convert to unix (seconds since epoch)
	gteUnix := gteTime.Unix()
	// push to the first 32 bits of the time, for parity with mongo timestamps
	gteTS := (gteUnix << 32)

	// build the oplog query
	oplogQuery := bson.M{
	//	"ts": bson.M{
	//		"$gte": gteTS,
	//	},
	}

	fmt.Printf("GTE Timestamp: %v\n", gteTS)

	// TODO: wait time
	return oplog.Find(oplogQuery).Iter()

}
