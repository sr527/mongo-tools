package options

import (
	"fmt"
)

type SourceOptions struct {
	From    string `long:"from" description:"specify the host for mongooplog to retrive operations from"`
	OplogNS string `long:"oplogns" description:"specify the namespace in the --from host where the oplog lives" default:"local.oplog.rs"`
}

func (self *SourceOptions) Name() string {
	return "source"
}

func (self *SourceOptions) Validate() error {
	if self.From == "" {
		return fmt.Errorf("need to specify --from")
	}
	return nil
}
