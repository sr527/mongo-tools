package main

import (
	"github.com/mongodb/mongo-tools/bsondump"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/util"
	"os"
)

func main() {
	// initialize command-line opts
	opts := options.New("bsondump", "<file>", options.EnabledOptions{})
	bsonDumpOpts := &bsondump.BSONDumpOptions{}
	opts.AddOptions(bsonDumpOpts)

	extra, err := opts.Parse()
	if err != nil {
		log.Logf(log.Always, "error parsing command line options: %v", err)
		os.Exit(util.ExitError)
	}

	// print help, if specified
	if opts.PrintHelp(false) {
		return
	}

	// print version, if specified
	if opts.PrintVersion() {
		return
	}

	log.SetVerbosity(opts.Verbosity)

	// pull out the filename
	filename := ""
	if len(extra) == 0 {
		opts.PrintHelp(true)
		return
	} else if len(extra) > 1 {
		log.Log(log.Always, "Too many positional operators.")
		opts.PrintHelp(true)
		os.Exit(util.ExitError)
	} else {
		filename = extra[0]
		if filename == "" {
			log.Log(log.Always, "Filename must not be blank.")
			opts.PrintHelp(true)
			os.Exit(util.ExitError)
		}
	}

	dumper := bsondump.BSONDump{
		ToolOptions:     opts,
		BSONDumpOptions: bsonDumpOpts,
		FileName:        filename,
		Out:             os.Stdout,
	}

	log.Logf(log.DebugLow, "running bsondump with objcheck: %v", bsonDumpOpts.ObjCheck)

	if len(bsonDumpOpts.Type) != 0 && bsonDumpOpts.Type != "debug" && bsonDumpOpts.Type != "json" {
		log.Logf(log.Always, "Unsupported output type '%v'. Must be either 'debug' or 'json'", bsonDumpOpts.Type)
		os.Exit(util.ExitBadOptions)
	}

	err = dumper.Open()
	if err != nil {
		log.Logf(log.Always, "Failed: %v", err)
		os.Exit(util.ExitError)
	}

	var numFound int
	if bsonDumpOpts.Type == "debug" {
		numFound, err = dumper.Debug()
	} else {
		numFound, err = dumper.Dump()
	}

	log.Logf(log.Always, "%v objects found", numFound)
	if err != nil {
		log.Log(log.Always, err.Error())
		os.Exit(util.ExitError)
	}
}
