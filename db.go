package boltplus

import (
	"bytes"
	"encoding/json"
	"errors"
	"log"
	"strings"

	"github.com/boltdb/bolt"
	"github.com/golang/snappy"
	"github.com/nytlabs/gojee"
)

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

// Close closes the db
func (db *DB) Close() {
	db.db.Close()
}

// Put inserts a doc into a bucket
func (db *DB) Put(bucketPath, key string, val map[string]interface{}) error {
	if err := db.db.Update(func(tx *bolt.Tx) error {
		bucket, err := db.getBucketOrCreate(tx, bucketPath)
		if err != nil {
			log.Print("bucket err:", err)
			return err
		}
		bs, err := db.dataToBytes(val)
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte(key), bs); err != nil {
			log.Print("put err:", err)
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// Get retrieves a doc from a bucket
func (db *DB) Get(bucketPath, key string) (map[string]interface{}, error) {
	resultChan := make(chan map[string]interface{}, 1)
	if err := db.db.View(func(tx *bolt.Tx) error {
		bucket, err := db.getBucket(tx, bucketPath)
		if err != nil {
			return err
		}
		data := bucket.Get([]byte(key))
		value, err := db.bytesToData(data)
		if err != nil {
			return err
		}
		resultChan <- value
		return nil
	}); err != nil {
		return nil, err
	}
	return <-resultChan, nil
}

// Delete deletes a doc from a bucket
func (db *DB) Delete(bucketPath, key string) error {
	if err := db.db.Update(func(tx *bolt.Tx) error {
		bucket, err := db.getBucket(tx, bucketPath)
		if err != nil {
			return err
		}
		if err := bucket.Delete([]byte(key)); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}
	return nil
}

// GetBucketContent returns all docs in a bucket
func (db *DB) GetBucketContent(bucketPath string) (chan *Pair, error) {
	returnChannel := make(chan *Pair, 64)

	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}

	bucket, err := db.getBucket(tx, bucketPath)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(returnChannel)
		defer tx.Rollback()
		e := bucket.ForEach(func(k, v []byte) error {
			if v != nil {
				value, e := db.bytesToData(v)
				if e != nil {
					return e
				}
				returnChannel <- &Pair{string(k), value}
			}
			return nil
		})
		if e != nil {
			log.Print(e)
		}
	}()

	return returnChannel, nil
}

// GetPrefix returns all docs in a bucket matching a prefix
func (db *DB) GetPrefix(bucketPath, prefix string) (chan *Pair, error) {
	returnChannel := make(chan *Pair, 64)

	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}

	bucket, err := db.getBucket(tx, bucketPath)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(returnChannel)
		defer tx.Rollback()
		c := bucket.Cursor()
		for k, v := c.Seek([]byte(prefix)); bytes.HasPrefix(k, []byte(prefix)); k, v = c.Next() {
			if v != nil {
				value, e := db.bytesToData(v)
				if e != nil {
					log.Print(e)
					continue
				}
				returnChannel <- &Pair{string(k), value}
			}
		}
	}()

	return returnChannel, nil
}

// GetRange returns all docs in a bucket matching a prefix
func (db *DB) GetRange(bucketPath, start, end string) (chan *Pair, error) {
	returnChannel := make(chan *Pair, 64)

	tx, err := db.db.Begin(false)
	if err != nil {
		return nil, err
	}

	bucket, err := db.getBucket(tx, bucketPath)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(returnChannel)
		defer tx.Rollback()
		c := bucket.Cursor()
		for k, v := c.Seek([]byte(start)); k != nil && bytes.Compare(k, []byte(end)) <= 0; k, v = c.Next() {
			if v != nil {
				value, e := db.bytesToData(v)
				if e != nil {
					log.Print(e)
					continue
				}
				returnChannel <- &Pair{string(k), value}
			}
		}
	}()

	return returnChannel, nil
}

// Find searches a bucket for documents
func (db *DB) Find(bucketPath, filterExpression string) (chan *Pair, error) {
	stream, err := db.GetBucketContent(bucketPath)
	if err != nil {
		return nil, err
	}
	return db.findFromStream(stream, filterExpression)
}

// FindPrefix searches a bucket for documents
func (db *DB) FindPrefix(bucketPath, prefix, filterExpression string) (chan *Pair, error) {
	stream, err := db.GetPrefix(bucketPath, prefix)
	if err != nil {
		return nil, err
	}
	return db.findFromStream(stream, filterExpression)
}

// FindRange searches a bucket for documents
func (db *DB) FindRange(bucketPath, start, end, filterExpression string) (chan *Pair, error) {
	stream, err := db.GetRange(bucketPath, start, end)
	if err != nil {
		return nil, err
	}
	return db.findFromStream(stream, filterExpression)
}

func (db *DB) open(filename string) error {
	dbHandle, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		return err
	}
	db.db = dbHandle
	return nil
}

func (db *DB) getBucketOrCreate(tx *bolt.Tx, bucketPath string) (*bolt.Bucket, error) {
	buckets := strings.Split(bucketPath, ".")
	bucket, err := tx.CreateBucketIfNotExists([]byte(buckets[0]))
	if err != nil {
		return nil, err
	}
	if len(bucketPath) > 1 {
		for _, id := range buckets {
			bucket, err = bucket.CreateBucketIfNotExists([]byte(id))
			if err != nil {
				return nil, err
			}
		}
	}
	return bucket, nil
}

func (db *DB) getBucket(tx *bolt.Tx, bucketPath string) (*bolt.Bucket, error) {
	buckets := strings.Split(bucketPath, ".")
	bucket := tx.Bucket([]byte(buckets[0]))
	if bucket == nil {
		return nil, errors.New("no such bucket")
	}
	if len(bucketPath) > 1 {
		for _, id := range buckets {
			bucket = bucket.Bucket([]byte(id))
			if bucket == nil {
				return nil, errors.New("no such bucket")
			}
		}
	}
	return bucket, nil
}

func (db *DB) dataToBytes(data map[string]interface{}) ([]byte, error) {
	var buff bytes.Buffer
	encoder := json.NewEncoder(snappy.NewWriter(&buff))
	if err := encoder.Encode(data); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (db *DB) bytesToData(data []byte) (map[string]interface{}, error) {
	buff := bytes.NewBuffer(data)
	decoder := json.NewDecoder(snappy.NewReader(buff))
	value := make(map[string]interface{})
	err := decoder.Decode(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (db *DB) findFromStream(stream chan *Pair, filterExpression string) (chan *Pair, error) {
	res := make(chan *Pair, 64)
	tokens, err := jee.Lexer(filterExpression)
	if err != nil {
		return nil, err
	}
	tree, err := jee.Parser(tokens)
	if err != nil {
		return nil, err
	}
	go func() {
		defer close(res)
		for pair := range stream {
			if val, err := jee.Eval(tree, pair.Value); err == nil {
				if match, ok := val.(bool); ok && match {
					res <- pair
				}
			} else {
				log.Print(err)
			}
		}
	}()
	return res, nil
}
