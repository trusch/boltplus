package boltplus

import (
	"io"

	"github.com/boltdb/bolt"
)

// DB wraps the boltdb handle
type DB struct {
	db *bolt.DB
}

type Object map[string]interface{}

// Pair is a key value pair
type Pair struct {
	Key   string                 `json:"key"`
	Value map[string]interface{} `json:"value"`
}

// New opens a database
func New(filename string) (*DB, error) {
	db := &DB{}
	return db, db.open(filename)
}

// Tx creates a new transaction. Do not forget to commit all writing transactions and to close read and write messages!
func (db *DB) Tx(writable bool) (*Transaction, error) {
	tx, err := db.db.Begin(writable)
	if err != nil {
		return nil, err
	}
	return &Transaction{tx: tx}, nil
}

// Close closes the db
func (db *DB) Close() {
	db.db.Close()
}

// Put inserts a doc into a bucket
func (db *DB) Put(bucketPath, key string, val Object) error {
	tx, err := db.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Close()
	err = tx.Put(bucketPath, key, val)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// Get retrieves a doc from a bucket
func (db *DB) Get(bucketPath, key string) (Object, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	return tx.Get(bucketPath, key)
}

// Delete deletes a doc from a bucket
func (db *DB) Delete(bucketPath, key string) error {
	tx, err := db.Tx(true)
	if err != nil {
		return err
	}
	defer tx.Close()
	err = tx.Delete(bucketPath, key)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// GetAll returns all docs in a bucket
func (db *DB) GetAll(bucketPath string) (chan *Pair, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	return tx.GetAll(bucketPath)
}

// GetPrefix returns all docs in a bucket matching a prefix
func (db *DB) GetPrefix(bucketPath, prefix string) (chan *Pair, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	return tx.GetPrefix(bucketPath, prefix)
}

// GetRange returns all docs in a bucket matching a prefix
func (db *DB) GetRange(bucketPath, start, end string) (chan *Pair, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	return tx.GetRange(bucketPath, start, end)
}

// Find searches a bucket for documents
func (db *DB) Find(bucketPath, filterExpression string) (chan *Pair, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	return tx.Find(bucketPath, filterExpression)
}

// FindPrefix searches a bucket for documents
func (db *DB) FindPrefix(bucketPath, prefix, filterExpression string) (chan *Pair, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	return tx.FindPrefix(bucketPath, prefix, filterExpression)
}

// FindRange searches a bucket for documents
func (db *DB) FindRange(bucketPath, start, end, filterExpression string) (chan *Pair, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	return tx.FindRange(bucketPath, start, end, filterExpression)
}

// Backup performs a hot backup of the whole database
func (db *DB) Backup(target io.Writer) error {
	tx, err := db.Tx(false)
	if err != nil {
		return err
	}
	defer tx.Close()
	return tx.Backup(target)
}

// Size return the size of the db in Bytes
func (db *DB) Size() (int64, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return 0, err
	}
	defer tx.Close()
	return tx.tx.Size(), nil
}

// Buckets returns a list of all buckets and subbuckets
func (db *DB) Buckets() ([]string, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	defer tx.Close()
	return tx.Buckets()
}

func (db *DB) open(filename string) error {
	dbHandle, err := bolt.Open(filename, 0600, nil)
	if err != nil {
		return err
	}
	db.db = dbHandle
	return nil
}
