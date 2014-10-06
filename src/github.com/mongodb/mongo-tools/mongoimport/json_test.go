package mongoimport

import (
	"bytes"
	. "github.com/smartystreets/goconvey/convey"
	"gopkg.in/mgo.v2/bson"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func TestJSONArrayReadDocument(t *testing.T) {
	Convey("With a JSON array import input", t, func() {
		readDocChan := make(chan bson.M, 1)
		var jsonFile, fileHandle *os.File

		Convey("an error should be thrown if a plain JSON document is supplied",
			func() {
				contents := `{"a": "ae"}`
				So(NewJSONInputReader(true, bytes.NewReader([]byte(contents))).ReadDocument(readDocChan), ShouldNotBeNil)
			})

		Convey("reading a JSON object that has no opening bracket should "+
			"error out",
			func() {
				contents := `{"a":3},{"b":4}]`
				So(NewJSONInputReader(true, bytes.NewReader([]byte(contents))).ReadDocument(readDocChan), ShouldNotBeNil)
			})

		Convey("JSON arrays that do not end with a closing bracket should "+
			"error out",
			func() {
				contents := `[{"a": "ae"}`
				fileHandle := bytes.NewReader([]byte(contents))
				So(NewJSONInputReader(true, fileHandle).ReadDocument(readDocChan), ShouldBeNil)
				So(NewJSONInputReader(true, fileHandle).ReadDocument(readDocChan), ShouldNotBeNil)
			})

		// TODO: we'll accept inputs like [[{},{}]] and just do nothing instead
		// of alerting the user of an error
		Convey("an error should be thrown if a plain JSON file is supplied",
			func() {
				fileHandle, err := os.Open("testdata/test_plain.json")
				So(err, ShouldBeNil)
				So(NewJSONInputReader(true, fileHandle).ReadDocument(readDocChan), ShouldNotBeNil)
			})

		Convey("array JSON input file sources should be parsed correctly and "+
			"subsequent imports should parse correctly",
			func() {
				// TODO: currently parses JSON as floats and not ints
				expectedReadOne := bson.M{"a": 1.2, "b": "a", "c": 0.4}
				expectedReadTwo := bson.M{"a": 2.4, "b": "string", "c": 52.9}
				fileHandle, err := os.Open("testdata/test_array.json")
				So(err, ShouldBeNil)
				jsonImporter := NewJSONInputReader(true, fileHandle)
				So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
				So(<-readDocChan, ShouldResemble, expectedReadOne)
				So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
				So(<-readDocChan, ShouldResemble, expectedReadTwo)
			})

		Reset(func() {
			jsonFile.Close()
			fileHandle.Close()
		})
	})
}

func TestJSONPlainReadDocument(t *testing.T) {
	Convey("With a plain JSON import input", t, func() {
		var err error
		readDocChan := make(chan bson.M, 1)
		var jsonFile, fileHandle *os.File

		Convey("string valued JSON documents should be imported properly",
			func() {
				contents := `{"a": "ae"}`
				expectedRead := bson.M{"a": "ae"}
				jsonFile, err = ioutil.TempFile("", "mongoimport_")
				jsonImporter := NewJSONInputReader(false, bytes.NewReader([]byte(contents)))
				So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
				So(<-readDocChan, ShouldResemble, expectedRead)
			})

		Convey("several string valued JSON documents should be imported "+
			"properly", func() {
			contents := `{"a": "ae"}{"b": "dc"}`
			expectedReadOne := bson.M{"a": "ae"}
			expectedReadTwo := bson.M{"b": "dc"}
			jsonImporter := NewJSONInputReader(false, bytes.NewReader([]byte(contents)))
			So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
			So(<-readDocChan, ShouldResemble, expectedReadOne)
			So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
			So(<-readDocChan, ShouldResemble, expectedReadTwo)
		})

		Convey("number valued JSON documents should be imported properly",
			func() {
				contents := `{"a": "ae", "b": 2.0}`
				expectedRead := bson.M{"a": "ae", "b": 2.0}
				jsonImporter := NewJSONInputReader(false, bytes.NewReader([]byte(contents)))
				So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
				So(<-readDocChan, ShouldResemble, expectedRead)
			})

		Convey("JSON arrays should return an error", func() {
			contents := `[{"a": "ae", "b": 2.0}]`
			jsonImporter := NewJSONInputReader(false, bytes.NewReader([]byte(contents)))
			So(jsonImporter.ReadDocument(readDocChan), ShouldNotBeNil)
		})

		Convey("plain JSON input file sources should be parsed correctly and "+
			"subsequent imports should parse correctly",
			func() {
				expectedReadOne := bson.M{"a": 4, "b": "string value", "c": 1}
				expectedReadTwo := bson.M{"a": 5, "b": "string value", "c": 2}
				fileHandle, err := os.Open("testdata/test_plain.json")
				So(err, ShouldBeNil)
				jsonImporter := NewJSONInputReader(false, fileHandle)
				So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
				So(<-readDocChan, ShouldNotResemble, expectedReadOne)
				So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
				So(<-readDocChan, ShouldNotResemble, expectedReadTwo)
			})

		Reset(func() {
			jsonFile.Close()
			fileHandle.Close()
		})
	})
}

func TestReadJSONArraySeparator(t *testing.T) {
	Convey("With an array JSON import input", t, func() {
		readDocChan := make(chan bson.M, 2)
		Convey("reading a JSON array separator should consume [",
			func() {
				contents := `[{"a": "ae"}`
				jsonImporter := NewJSONInputReader(true, bytes.NewReader([]byte(contents)))
				So(jsonImporter.readJSONArraySeparator(), ShouldBeNil)
				// at this point it should have consumed all bytes up to `{`
				So(jsonImporter.readJSONArraySeparator(), ShouldNotBeNil)
			})
		Convey("reading a closing JSON array separator without a "+
			"corresponding opening bracket should error out ",
			func() {
				contents := `]`
				jsonImporter := NewJSONInputReader(true, bytes.NewReader([]byte(contents)))
				So(jsonImporter.readJSONArraySeparator(), ShouldNotBeNil)
			})
		Convey("reading an opening JSON array separator without a "+
			"corresponding closing bracket should error out ",
			func() {
				contents := `[`
				jsonImporter := NewJSONInputReader(true, bytes.NewReader([]byte(contents)))
				So(jsonImporter.readJSONArraySeparator(), ShouldBeNil)
				So(jsonImporter.readJSONArraySeparator(), ShouldNotBeNil)
			})
		Convey("reading an opening JSON array separator with an ending "+
			"closing bracket should return EOF",
			func() {
				contents := `[]`
				jsonImporter := NewJSONInputReader(true, bytes.NewReader([]byte(contents)))
				So(jsonImporter.readJSONArraySeparator(), ShouldBeNil)
				So(jsonImporter.readJSONArraySeparator(), ShouldEqual, io.EOF)
			})
		Convey("reading an opening JSON array separator, an ending closing "+
			"bracket but then additional characters after that, should error",
			func() {
				contents := `[]a`
				jsonImporter := NewJSONInputReader(true, bytes.NewReader([]byte(contents)))
				So(jsonImporter.readJSONArraySeparator(), ShouldBeNil)
				So(jsonImporter.readJSONArraySeparator(), ShouldNotBeNil)
			})
		Convey("reading invalid JSON objects between valid objects should "+
			"error out",
			func() {
				contents := `[{"a":3}x{"b":4}]`
				jsonImporter := NewJSONInputReader(true, bytes.NewReader([]byte(contents)))
				So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
				So(jsonImporter.readJSONArraySeparator(), ShouldNotBeNil)
			})
		Convey("reading invalid JSON objects after valid objects but between "+
			"valid objects should error out",
			func() {
				contents := `[{"a":3},b{"b":4}]`
				jsonImporter := NewJSONInputReader(true, bytes.NewReader([]byte(contents)))
				So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
				So(jsonImporter.readJSONArraySeparator(), ShouldBeNil)
				So(jsonImporter.readJSONArraySeparator(), ShouldNotBeNil)

				contents = `[{"a":3},,{"b":4}]`
				jsonImporter = NewJSONInputReader(true, bytes.NewReader([]byte(contents)))
				So(jsonImporter.ReadDocument(readDocChan), ShouldBeNil)
				So(jsonImporter.ReadDocument(readDocChan), ShouldNotBeNil)
			})
	})
}
