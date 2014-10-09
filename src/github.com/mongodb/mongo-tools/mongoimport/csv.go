package mongoimport

import (
	"fmt"
	"github.com/mongodb/mongo-tools/common/log"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongoimport/csv"
	"gopkg.in/mgo.v2/bson"
	"io"
	"strconv"
	"strings"
)

// CSVInputReader is a struct that implements the InputReader interface for a
// CSV input source
type CSVInputReader struct {
	// Fields is a list of field names in the BSON documents to be imported
	Fields []string
	// csvReader is the underlying reader used to read data in from the CSV
	// or TSV file
	csvReader *csv.Reader
	// numProcessed indicates the number of CSV documents processed
	numProcessed int64
	// csvRecord stores each line of input we read from the underlying reader
	csvRecord []string
	// document is used to hold the decoded JSON document as a bson.M
	document bson.M
}

// NewCSVInputReader returns a CSVInputReader configured to read input from the
// given io.Reader, extracting the specified fields only.
func NewCSVInputReader(fields []string, in io.Reader) *CSVInputReader {
	csvReader := csv.NewReader(in)
	// allow variable number of fields in document
	csvReader.FieldsPerRecord = -1
	csvReader.TrimLeadingSpace = true
	return &CSVInputReader{
		Fields:    fields,
		csvReader: csvReader,
	}
}

// SetHeader sets the header field for a CSV
func (csvImporter *CSVInputReader) SetHeader(hasHeaderLine bool) (err error) {
	fields, err := validateHeaders(csvImporter, hasHeaderLine)
	if err != nil {
		return err
	}
	csvImporter.Fields = fields
	return nil
}

// GetHeaders returns the current header fields for a CSV importer
func (csvImporter *CSVInputReader) GetHeaders() []string {
	return csvImporter.Fields
}

// ReadHeadersFromSource reads the header field from the CSV importer's reader
func (csvImporter *CSVInputReader) ReadHeadersFromSource() ([]string, error) {
	return csvImporter.csvReader.Read()
}

// ReadDocument reads a line of input with the CSV representation of a document
// and writes the BSON equivalent to the provided channel
func (csvImporter *CSVInputReader) ReadDocument() (map[string]interface{}, error) {
	csvImporter.numProcessed++
	csvRecord, err := csvImporter.csvReader.Read()
	if err != nil {
		if err == io.EOF {
			return nil, err
		}
		return nil, fmt.Errorf("read error on entry #%v: %v", csvImporter.numProcessed, err)
	}
	log.Logf(2, "got line: %v", strings.Join(csvRecord, ","))
	var key string
	document := map[string]interface{}{}
	for index, token := range csvRecord {
		parsedValue := getParsedValue(token)
		if index < len(csvImporter.Fields) {
			// for nested fields - in the form "a.b.c", ensure
			// that the value is set accordingly
			if strings.Contains(csvImporter.Fields[index], ".") {
				setNestedValue(csvImporter.Fields[index], parsedValue, csvImporter.document)
			} else {
				csvImporter.document[csvImporter.Fields[index]] = parsedValue
			}
		} else {
			key = "field" + strconv.Itoa(index)
			if util.StringSliceContains(csvImporter.Fields, key) {
				return nil, fmt.Errorf("Duplicate header name - on %v - for token #%v ('%v') in document #%v",
					key, index+1, parsedValue, csvImporter.numProcessed)
			}
			csvImporter.document[key] = parsedValue
		}
	}
	return document, nil
	//readDocChan <- csvImporter.document
	//return nil
}
