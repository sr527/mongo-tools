package mongoimport

import (
	"bufio"
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/util"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strconv"
	"strings"
)

const (
	entryDelimiter = '\n'
	tokenSeparator = "\t"
)

// TSVInputReader is a struct that implements the InputReader interface for a
// TSV input source
type TSVInputReader struct {
	// Fields is a list of field names in the BSON documents to be imported
	Fields []string
	// tsvReader is the underlying reader used to read data in from the TSV
	// or TSV file
	tsvReader *bufio.Reader
	// numProcessed indicates the number of TSV documents processed
	numProcessed int64
	// tsvRecord stores each line of input we read from the underlying reader
	tsvRecord string
	// document is used to hold the decoded JSON document as a bson.M
	document bson.M
}

// NewTSVInputReader returns a TSVInputReader configured to read input from the
// given io.Reader, extracting the specified fields only.
func NewTSVInputReader(fields []string, in io.Reader) *TSVInputReader {
	return &TSVInputReader{
		Fields:    fields,
		tsvReader: bufio.NewReader(in),
	}
}

// SetHeader sets the header field for a TSV
func (tsvImporter *TSVInputReader) SetHeader(hasHeaderLine bool) (err error) {
	fields, err := validateHeaders(tsvImporter, hasHeaderLine)
	if err != nil {
		return err
	}
	tsvImporter.Fields = fields
	return nil
}

// GetHeaders returns the current header fields for a TSV importer
func (tsvImporter *TSVInputReader) GetHeaders() []string {
	return tsvImporter.Fields
}

// ReadHeadersFromSource reads the header field from the TSV importer's reader
func (tsvImporter *TSVInputReader) ReadHeadersFromSource() ([]string, error) {
	unsortedHeaders := []string{}
	stringHeaders, err := tsvImporter.tsvReader.ReadString(entryDelimiter)
	if err != nil {
		return nil, err
	}
	tokenizedHeaders := strings.Split(stringHeaders, tokenSeparator)
	for _, header := range tokenizedHeaders {
		unsortedHeaders = append(unsortedHeaders, strings.TrimSpace(header))
	}
	return unsortedHeaders, nil
}

// ReadDocument reads a line of input with the TSV representation of a document
// and writes the BSON equivalent to the provided channel
func (tsvImporter *TSVInputReader) ReadDocument(readDocChan chan bson.M) (err error) {
	tsvImporter.numProcessed++
	tsvImporter.tsvRecord, err = tsvImporter.tsvReader.ReadString(entryDelimiter)
	if err != nil {
		if err == io.EOF {
			return err
		}
		return fmt.Errorf("read error on entry #%v: %v", tsvImporter.numProcessed, err)
	}
	log.Logf(2, "got line: %v", tsvImporter.tsvRecord)

	// strip the trailing '\r\n' from ReadString
	if len(tsvImporter.tsvRecord) != 0 {
		tsvImporter.tsvRecord = strings.TrimRight(tsvImporter.tsvRecord, "\r\n")
	}
	tsvImporter.document = bson.M{}
	var key string
	for index, token := range strings.Split(tsvImporter.tsvRecord, tokenSeparator) {
		parsedValue := getParsedValue(token)
		if index < len(tsvImporter.Fields) {
			if strings.Contains(tsvImporter.Fields[index], ".") {
				setNestedValue(tsvImporter.Fields[index], parsedValue, tsvImporter.document)
			} else {
				tsvImporter.document[tsvImporter.Fields[index]] = parsedValue
			}
		} else {
			key = "field" + strconv.Itoa(index)
			if util.StringSliceContains(tsvImporter.Fields, key) {
				return fmt.Errorf("Duplicate header name - on %v - for token #%v ('%v') in document #%v",
					key, index+1, parsedValue, tsvImporter.numProcessed)
			}
			tsvImporter.document[key] = parsedValue
		}
	}
	readDocChan <- tsvImporter.document
	return nil
}
