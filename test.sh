#!/bin/bash

# exit on error
set -e

TOOLS_PKG='github.com/mongodb/mongo-tools'

# what is to be tested
testdir="$1"
otheropts="${@:2}"

# make sure we're in the directory where the make script lives
SCRIPT_DIR="$(cd "$(dirname ${BASH_SOURCE[0]})" && pwd)"
cd $SCRIPT_DIR
echo "Testing directory $testdir"

# set up the $GOPATH to be able to reference the vendored dependencies
rm -rf .gopath/
mkdir -p .gopath/src/"$(dirname $TOOLS_PKG)"
ln -sf `pwd` .gopath/src/$TOOLS_PKG
export GOPATH=`pwd`/.gopath:`pwd`/vendor

# run the specified tests
cd $testdir
go test -i 
go test -v "${otheropts[@]}"
