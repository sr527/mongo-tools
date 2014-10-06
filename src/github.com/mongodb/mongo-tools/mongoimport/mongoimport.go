package mongoimport

import (
	"errors"
	"fmt"
	"github.com/mongodb/mongo-tools/common/db"
	"github.com/mongodb/mongo-tools/common/log"
	commonOpts "github.com/mongodb/mongo-tools/common/options"
	"github.com/mongodb/mongo-tools/common/util"
	"github.com/mongodb/mongo-tools/mongoimport/options"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// input type constants
const (
	CSV  = "csv"
	TSV  = "tsv"
	JSON = "json"
)

// ingestion constants
const (
	maxMessageSizeBytes = 32 * 1000 * 1000
	batchSize           = 100000 // TODO: make this configurable
)

// compile-time interface sanity check
var (
	_ InputReader = (*CSVInputReader)(nil)
	_ InputReader = (*TSVInputReader)(nil)
	_ InputReader = (*JSONInputReader)(nil)
)

var (
	errNsNotFound        = errors.New("ns not found")
	errNoReachableServer = errors.New("no reachable servers")
)

// Wrapper for MongoImport functionality
type MongoImport struct {
	// generic mongo tool options
	ToolOptions *commonOpts.ToolOptions

	// InputOptions defines options used to read data to be ingested
	InputOptions *options.InputOptions

	// IngestOptions defines options used to ingest data into MongoDB
	IngestOptions *options.IngestOptions

	// SessionProvider is used for connecting to the database
	SessionProvider *db.SessionProvider
}

// InputReader is an interface that specifies how an input source should be
// converted to BSON
type InputReader interface {
	// ReadDocument reads the given record from the given io.Reader according
	// to the format supported by the underlying InputReader implementation.kk
	ReadDocument(chan bson.M) error

	// SetHeader sets the header for the CSV/TSV import when --headerline is
	// specified. It a --fields or --fieldFile argument is passed, it overwrites
	// the values of those with what is read from the input source
	SetHeader(bool) error

	// ReadHeadersFromSource attempts to reads the header fields for the specific implementation
	ReadHeadersFromSource() ([]string, error)

	// GetHeaders returns the current set of header fields for the specific implementation
	GetHeaders() []string
}

// ValidateSettings ensures that the tool specific options supplied for
// MongoImport are valid
func (mongoImport *MongoImport) ValidateSettings(args []string) error {
	if err := mongoImport.ToolOptions.Validate(); err != nil {
		return err
	}

	// Namespace must have a valid database if none is specified,
	// use 'test'
	if mongoImport.ToolOptions.Namespace.DB == "" {
		mongoImport.ToolOptions.Namespace.DB = "test"
	} else {
		if err := util.ValidateDBName(mongoImport.ToolOptions.Namespace.DB); err != nil {
			return err
		}
	}

	// use JSON as default input type
	if mongoImport.InputOptions.Type == "" {
		mongoImport.InputOptions.Type = JSON
	} else {
		if !(mongoImport.InputOptions.Type == TSV ||
			mongoImport.InputOptions.Type == JSON ||
			mongoImport.InputOptions.Type == CSV) {
			return fmt.Errorf("don't know what type [\"%v\"] is",
				mongoImport.InputOptions.Type)
		}
	}

	// ensure headers are supplied for CSV/TSV
	if mongoImport.InputOptions.Type == CSV ||
		mongoImport.InputOptions.Type == TSV {
		if !mongoImport.InputOptions.HeaderLine {
			if mongoImport.InputOptions.Fields == "" &&
				mongoImport.InputOptions.FieldFile == "" {
				return fmt.Errorf("You need to specify fields or have a " +
					"header line to import this file type")
			}
			if mongoImport.InputOptions.Fields != "" &&
				mongoImport.InputOptions.FieldFile != "" {
				return fmt.Errorf("incompatible options: --fields and --fieldFile")
			}
		} else {
			if mongoImport.InputOptions.Fields != "" {
				return fmt.Errorf("incompatible options: --fields and --headerline")
			}
			if mongoImport.InputOptions.FieldFile != "" {
				return fmt.Errorf("incompatible options: --fieldFile and --headerline")
			}
		}
	}
	if len(args) > 1 {
		return fmt.Errorf("too many positional arguments")
	}
	if mongoImport.InputOptions.File != "" && len(args) != 0 {
		return fmt.Errorf(`multiple occurrences of option "--file"`)
	}
	var fileBaseName string
	if mongoImport.InputOptions.File != "" {
		fileBaseName = mongoImport.InputOptions.File
	} else {
		if len(args) != 0 {
			fileBaseName = args[0]
			mongoImport.InputOptions.File = args[0]
		}
	}

	if mongoImport.ToolOptions.DBPath != "" {
		return fmt.Errorf("--dbpath is now deprecated. start a mongod instead")
	}

	// ensure we have a valid string to use for the collection
	if mongoImport.ToolOptions.Namespace.Collection == "" {
		if fileBaseName == "" {
			return fmt.Errorf("no collection specified")
		}
		fileBaseName = filepath.Base(fileBaseName)
		if lastDotIndex := strings.LastIndex(fileBaseName, "."); lastDotIndex != -1 {
			fileBaseName = fileBaseName[0:lastDotIndex]
		}
		if err := util.ValidateCollectionName(fileBaseName); err != nil {
			return err
		}
		mongoImport.ToolOptions.Namespace.Collection = fileBaseName
		log.Logf(0, "no collection specified")
		log.Logf(0, "using filename '%v' as collection",
			mongoImport.ToolOptions.Namespace.Collection)
	}
	return nil
}

// getSourceReader returns an io.Reader to read from the input source
func (mongoImport *MongoImport) getSourceReader() (io.ReadCloser, error) {
	if mongoImport.InputOptions.File != "" {
		file, err := os.Open(util.ToUniversalPath(mongoImport.InputOptions.File))
		if err != nil {
			return nil, err
		}
		fileStat, err := file.Stat()
		if err != nil {
			return nil, err
		}
		log.Logf(1, "filesize: %v", fileStat.Size())
		return file, err
	}
	log.Logf(1, "filesize: 0")
	return os.Stdin, nil
}

// ImportDocuments is used to write input data to the database. It returns the
// number of documents successfully imported to the appropriate namespace and
// any error encountered in doing this
func (mongoImport *MongoImport) ImportDocuments() (int64, error) {
	in, err := mongoImport.getSourceReader()
	if err != nil {
		return 0, err
	}
	defer in.Close()

	inputReader, err := mongoImport.getInputReader(in)
	if err != nil {
		return 0, err
	}

	err = inputReader.SetHeader(mongoImport.InputOptions.HeaderLine)
	if err != nil {
		return 0, err
	}
	return mongoImport.importDocuments(inputReader)
}

// importDocuments is a helper to ImportDocuments and does all the ingestion
// work by taking data from the 'inputReader' source and writing it to the
// appropriate namespace
func (mongoImport *MongoImport) importDocuments(inputReader InputReader) (docsCount int64, err error) {
	connURL := mongoImport.ToolOptions.Host
	if connURL == "" {
		connURL = util.DefaultHost
	}
	var readErr error
	session, err := mongoImport.SessionProvider.GetSession()
	if err != nil {
		return 0, fmt.Errorf("error connecting to mongod: %v", err)
	}
	session.SetSocketTimeout(0)
	defer func() {
		session.Close()
		if readErr != nil && readErr == io.EOF {
			readErr = nil
		}
		if err == nil {
			err = readErr
		}
	}()

	if mongoImport.ToolOptions.Port != "" {
		connURL = connURL + ":" + mongoImport.ToolOptions.Port
	}
	log.Logf(0, "connected to: %v", connURL)

	collection := session.DB(mongoImport.ToolOptions.DB).C(mongoImport.ToolOptions.Collection)

	log.Logf(1, "ns: %v.%v",
		mongoImport.ToolOptions.Namespace.DB,
		mongoImport.ToolOptions.Namespace.Collection)

	// drop the database if necessary
	if mongoImport.IngestOptions.Drop {
		log.Logf(0, "dropping: %v.%v",
			mongoImport.ToolOptions.DB,
			mongoImport.ToolOptions.Collection)

		if err := collection.DropCollection(); err != nil {
			if err.Error() != errNsNotFound.Error() {
				return 0, err
			}
		}
	}

	readDocChan := make(chan bson.M, batchSize)
	readErrChan := make(chan error)

	go func() {
		for {
			if err = inputReader.ReadDocument(readDocChan); err != nil {
				if err == io.EOF || mongoImport.IngestOptions.StopOnError {
					close(readDocChan)
					readErrChan <- err
					return
				}
				log.Logf(0, "error reading document: %v", err)
			}
		}
	}()
	docsCount, err = mongoImport.IngestDocuments(readDocChan, collection)
	readErr = <-readErrChan
	return docsCount, err
}

// IngestDocuments takes a slice of documents and either inserts/upserts them -
// based on whether an upsert is requested - into the given collection
func (mongoImport *MongoImport) IngestDocuments(readDocChan chan bson.M, collection *mgo.Collection) (docsCount int64, err error) {
	ignoreBlanks := mongoImport.IngestOptions.IgnoreBlanks && mongoImport.InputOptions.Type != JSON

	numMessageBytes := 0
	wg := &sync.WaitGroup{}
	documentBytes := make([]byte, 0)
	ingestErrChan := make(chan error, 100000000)
	documents := make([]interface{}, 0)

	for document := range readDocChan {
		/*
			docsCount++
			if docsCount == batchSize {
				log.Logf(0, "Progress: %v documents inserted...", docsCount)
			}
			continue
		*/
		// ignore blank fields if specified
		if ignoreBlanks {
			document = removeBlankFields(document)
		}
		if documentBytes, err = bson.Marshal(document); err != nil {
			return docsCount, err
		}
		numMessageBytes += len(documentBytes)
		documents = append(documents, document)

		// send documents over the wire when we hit the batch size or are at/over
		// the maximum message size threshold
		if len(documents) == batchSize || numMessageBytes >= maxMessageSizeBytes {
			if !mongoImport.IngestOptions.Concurrent {
				if err = mongoImport.ingestDocuments(documents, collection); err != nil {
					// return immediately if StopOnError is set or the server can't be reached
					log.Logf(0, "error inserting documents: %v", err)
					if err.Error() == errNoReachableServer.Error() || mongoImport.IngestOptions.StopOnError {
						return
					}
					err = nil
				} else {
					// TODO: what if some documents were inserted?
					docsCount += int64(len(documents))
				}
			} else {
				wg.Add(1)
				go mongoImport.ingestDocumentsConcurrently(documents, wg, ingestErrChan)
				// TODO: what if some documents were inserted?
				docsCount += int64(len(documents))
			}
			log.Logf(0, "Progress: %v documents inserted...", docsCount)
			documents = documents[:0]
			numMessageBytes = 0
		}
	}
	// wait for any goroutines to complete
	wg.Wait()

	// write any documents left in slice
	if len(documents) != 0 {
		if err = mongoImport.ingestDocuments(documents, collection); err != nil {
			log.Logf(0, "error inserting documents: %v", err)
			if err.Error() == errNoReachableServer.Error() || mongoImport.IngestOptions.StopOnError {
				return
			}
			err = nil
		} else {
			// TODO: what if some documents were inserted?
			docsCount += int64(len(documents))
		}
	}
	return
}

// ingestDocuments is a helper to IngestDocuments - the actual insertion/updates
// happen here. If no upsert fields are found, it simply inserts the documents
// into the given collection
func (mongoImport *MongoImport) ingestDocuments(documents []interface{}, collection *mgo.Collection) (err error) {
	// TODO: get exact number of documents actually inserted
	if !mongoImport.IngestOptions.Upsert {
		return collection.Insert(documents...)
	}
	selector := bson.M{}
	upsertFields := strings.Split(mongoImport.IngestOptions.UpsertFields, ",")
	for _, document := range documents {
		selector = constructUpsertDocument(upsertFields, document.(bson.M))
		if selector == nil {
			err = collection.Insert(document)
		} else {
			_, err = collection.Upsert(selector, document.(bson.M))
		}
		if err != nil {
			return err
		}
	}
	return nil
}

// ingestDocumentsConcurrently ingests documents concurrently.
// TODO: this is currently all done over the same session
func (mongoImport *MongoImport) ingestDocumentsConcurrently(documents []interface{}, wg *sync.WaitGroup, ingestErrChan chan error) {
	// TODO: actually read errors on ingestErrChan
	session, err := mongoImport.SessionProvider.GetSession()
	if err != nil {
		ingestErrChan <- fmt.Errorf("error connecting to mongod: %v", err)
	}
	session.SetSocketTimeout(0)
	collection := session.DB(mongoImport.ToolOptions.DB).C(mongoImport.ToolOptions.Collection)
	ingestErrChan <- mongoImport.ingestDocuments(documents, collection)
	wg.Done()
}

// getInputReader returns an implementation of InputReader which can handle
// transforming TSV, CSV, or JSON into appropriate BSON documents
func (mongoImport *MongoImport) getInputReader(in io.Reader) (InputReader, error) {
	var fields []string
	var err error
	if len(mongoImport.InputOptions.Fields) != 0 {
		fields = strings.Split(strings.Trim(mongoImport.InputOptions.Fields, " "), ",")
	} else if mongoImport.InputOptions.FieldFile != "" {
		fields, err = util.GetFieldsFromFile(mongoImport.InputOptions.FieldFile)
		if err != nil {
			return nil, err
		}
	}
	if mongoImport.InputOptions.Type == CSV {
		return NewCSVInputReader(fields, in), nil
	} else if mongoImport.InputOptions.Type == TSV {
		return NewTSVInputReader(fields, in), nil
	}
	return NewJSONInputReader(mongoImport.InputOptions.JSONArray, in), nil
}
