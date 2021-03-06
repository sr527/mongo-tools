package main

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongooplog"
	"os"
)

func main() {

	// initialize command line options
	opts := options.New("mongooplog", "<options>", options.EnabledOptions{Auth: true, Connection: true, Namespace: false})

	// add the mongooplog-specific options
	sourceOpts := &mongooplog.SourceOptions{}
	opts.AddOptions(sourceOpts)

	// parse the command line options
	_, err := opts.Parse()
	if err != nil {
		fmt.Printf("error parsing command line: %v\n\n", err)
		fmt.Printf("try 'mongooplog --help' for more information\n")
		os.Exit(util.ExitBadOptions)
	}

	// print help, if specified
	if opts.PrintHelp(false) {
		return
	}

	// print version, if specified
	if opts.PrintVersion() {
		return
	}

	// init logger
	log.SetVerbosity(opts.Verbosity)

	// connect directly, unless a replica set name is explicitly specified
	_, setName := util.ParseConnectionString(opts.Host)
	opts.Direct = (setName == "")

	// validate the mongooplog options
	if err := sourceOpts.Validate(); err != nil {
		fmt.Printf("command line error: %v\n", err)
		os.Exit(util.ExitBadOptions)
	}

	// create a session provider for the destination server
	sessionProviderTo, err := db.NewSessionProvider(*opts)
	if err != nil {
		fmt.Printf("error connecting to destination host: %v", err)
		os.Exit(util.ExitError)
	}

	// create a session provider for the source server
	opts.Connection.Host = sourceOpts.From
	opts.Connection.Port = ""
	sessionProviderFrom, err := db.NewSessionProvider(*opts)
	if err != nil {
		fmt.Printf("error connecting to source host: %v\n", err)
		os.Exit(util.ExitError)
	}

	// initialize mongooplog
	oplog := mongooplog.MongoOplog{
		ToolOptions:         opts,
		SourceOptions:       sourceOpts,
		SessionProviderFrom: sessionProviderFrom,
		SessionProviderTo:   sessionProviderTo,
	}

	// kick it off
	if err := oplog.Run(); err != nil {
		fmt.Printf("error: %v\n", err)
		os.Exit(util.ExitError)
	}

}
