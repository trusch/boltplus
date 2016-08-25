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

// Transaction represents represents a bold db transaction and exposes the query and update routines
// usage:
// ```
// tx, err := db.NewTransaction()
// defer tx.Close()
// tx.Get(...)
// ...
// tx.Put(...)
// tx.Commit()
// ```
type Transaction struct {
	tx         *bolt.Tx
	isFinished bool
}

// Commit commits and closes the transaction
func (tx *Transaction) Commit() error {
	tx.isFinished = true
	return tx.tx.Commit()
}

// Rollback discards all changes and closes the transaction
func (tx *Transaction) Rollback() error {
	tx.isFinished = true
	return tx.tx.Rollback()
}

// Close closes the transaction. If neither Commit nor Rollback were called before,
// it rollbacks the transaction
func (tx *Transaction) Close() error {
	if !tx.isFinished {
		return tx.Rollback()
	}
	return nil
}

// Put inserts a doc into a bucket
func (tx *Transaction) Put(bucketPath, key string, val map[string]interface{}) error {
	bucket, err := tx.getBucketOrCreate(bucketPath)
	if err != nil {
		log.Print("bucket err:", err)
		return err
	}
	bs, err := tx.dataToBytes(val)
	if err != nil {
		return err
	}
	return bucket.Put([]byte(key), bs)
}

// Get retrieves a doc from a bucket
func (tx *Transaction) Get(bucketPath, key string) (map[string]interface{}, error) {
	bucket, err := tx.getBucket(bucketPath)
	if err != nil {
		return nil, err
	}
	data := bucket.Get([]byte(key))
	return tx.bytesToData(data)
}

// Delete deletes a doc from a bucket
func (tx *Transaction) Delete(bucketPath, key string) error {
	bucket, err := tx.getBucket(bucketPath)
	if err != nil {
		return err
	}
	return bucket.Delete([]byte(key))
}

// GetBucketContent returns all docs in a bucket
func (tx *Transaction) GetBucketContent(bucketPath string) (chan *Pair, error) {
	returnChannel := make(chan *Pair, 64)
	bucket, err := tx.getBucket(bucketPath)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(returnChannel)
		defer tx.Close()
		e := bucket.ForEach(func(k, v []byte) error {
			if v != nil {
				value, e := tx.bytesToData(v)
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
func (tx *Transaction) GetPrefix(bucketPath, prefix string) (chan *Pair, error) {
	returnChannel := make(chan *Pair, 64)

	bucket, err := tx.getBucket(bucketPath)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(returnChannel)
		defer tx.Close()
		c := bucket.Cursor()
		for k, v := c.Seek([]byte(prefix)); bytes.HasPrefix(k, []byte(prefix)); k, v = c.Next() {
			if v != nil {
				value, e := tx.bytesToData(v)
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
func (tx *Transaction) GetRange(bucketPath, start, end string) (chan *Pair, error) {
	returnChannel := make(chan *Pair, 64)

	bucket, err := tx.getBucket(bucketPath)
	if err != nil {
		return nil, err
	}

	go func() {
		defer close(returnChannel)
		defer tx.Close()
		c := bucket.Cursor()
		for k, v := c.Seek([]byte(start)); k != nil && bytes.Compare(k, []byte(end)) <= 0; k, v = c.Next() {
			if v != nil {
				value, e := tx.bytesToData(v)
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
func (tx *Transaction) Find(bucketPath, filterExpression string) (chan *Pair, error) {
	stream, err := tx.GetBucketContent(bucketPath)
	if err != nil {
		return nil, err
	}
	return tx.findFromStream(stream, filterExpression)
}

// FindPrefix searches a bucket for documents
func (tx *Transaction) FindPrefix(bucketPath, prefix, filterExpression string) (chan *Pair, error) {
	stream, err := tx.GetPrefix(bucketPath, prefix)
	if err != nil {
		return nil, err
	}
	return tx.findFromStream(stream, filterExpression)
}

// FindRange searches a bucket for documents
func (tx *Transaction) FindRange(bucketPath, start, end, filterExpression string) (chan *Pair, error) {
	stream, err := tx.GetRange(bucketPath, start, end)
	if err != nil {
		return nil, err
	}
	return tx.findFromStream(stream, filterExpression)
}

func (tx *Transaction) getBucketOrCreate(bucketPath string) (*bolt.Bucket, error) {
	buckets := strings.Split(bucketPath, ".")
	bucket, err := tx.tx.CreateBucketIfNotExists([]byte(buckets[0]))
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

func (tx *Transaction) getBucket(bucketPath string) (*bolt.Bucket, error) {
	buckets := strings.Split(bucketPath, ".")
	bucket := tx.tx.Bucket([]byte(buckets[0]))
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

func (tx *Transaction) dataToBytes(data map[string]interface{}) ([]byte, error) {
	var buff bytes.Buffer
	encoder := json.NewEncoder(snappy.NewWriter(&buff))
	if err := encoder.Encode(data); err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

func (tx *Transaction) bytesToData(data []byte) (map[string]interface{}, error) {
	buff := bytes.NewBuffer(data)
	decoder := json.NewDecoder(snappy.NewReader(buff))
	value := make(map[string]interface{})
	err := decoder.Decode(&value)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (tx *Transaction) findFromStream(stream chan *Pair, filterExpression string) (chan *Pair, error) {
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
