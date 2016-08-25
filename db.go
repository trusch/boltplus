package boltplus

import "github.com/boltdb/bolt"

// DB wraps the boltdb handle
type DB struct {
	db *bolt.DB
}

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
func (db *DB) Put(bucketPath, key string, val map[string]interface{}) error {
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
func (db *DB) Get(bucketPath, key string) (map[string]interface{}, error) {
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

// GetBucketContent returns all docs in a bucket
func (db *DB) GetBucketContent(bucketPath string) (chan *Pair, error) {
	tx, err := db.Tx(false)
	if err != nil {
		return nil, err
	}
	return tx.GetBucketContent(bucketPath)
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

func (db *DB) open(filename string) error {
	dbHandle, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		return err
	}
	db.db = dbHandle
	return nil
}
