package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"

	"github.com/trusch/boltplus"
)

var dbPath = flag.String("db", "default.db", "db to use")
var bucketPath = flag.String("bucket", "", "bucket to use. You can use dot-notation for nested buckets!")
var key = flag.String("key", "", "key to use")
var doc = flag.String("doc", "", "json doc to save")

var put = flag.Bool("put", false, "save")
var get = flag.Bool("get", false, "retrieve")
var delete = flag.Bool("delete", false, "delete")

var all = flag.Bool("all", false, "query all docs in a bucket")
var prefix = flag.String("prefix", "", "prefix to search")
var rangeStart = flag.String("rangeStart", "", "rangeStart to search")
var rangeEnd = flag.String("rangeEnd", "", "rangeEnd to search")

var filter = flag.String("filter", "", "filter returned docs with gojee")

func init() {
	flag.Parse()
	if !*all && !*put && !*get && !*delete && *prefix == "" && *rangeStart == "" && *rangeEnd == "" {
		if *bucketPath != "" && *key != "" && *doc != "" {
			*put = true
		} else if *bucketPath != "" && *key != "" {
			*get = true
		} else if *bucketPath != "" {
			*all = true
		} else {
			log.Fatal("please specify what to do")
		}
	}
}

func putCmd(db *boltplus.DB) {
	if *bucketPath == "" || *key == "" || *doc == "" {
		log.Fatal("specify bucket, key and doc")
	}
	docObj := make(map[string]interface{})
	err := json.Unmarshal([]byte(*doc), &docObj)
	if err != nil {
		log.Fatal(err)
	}
	err = db.Put(*bucketPath, *key, docObj)
	if err != nil {
		log.Fatal(err)
	}
}

func getCmd(db *boltplus.DB) {
	if *bucketPath == "" || *key == "" {
		log.Fatal("specify bucket and key")
	}
	val, err := db.Get(*bucketPath, *key)
	if err != nil {
		if err.Error() == "EOF" {
			log.Fatal("no such key in bucket")
		}
		log.Fatal(err)
	}
	data, err := json.Marshal(val)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(data))
}

func deleteCmd(db *boltplus.DB) {
	if *bucketPath == "" || *key == "" {
		log.Fatal("specify bucket and key")
	}
	err := db.Delete(*bucketPath, *key)
	if err != nil {
		log.Fatal(err)
	}
}

func getAllCmd(db *boltplus.DB) {
	if *bucketPath == "" {
		log.Fatal("specify bucket")
	}
	ch, err := db.GetBucketContent(*bucketPath)
	if err != nil {
		log.Fatal(err)
	}
	for val := range ch {
		data, err := json.Marshal(val)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(data))
	}
}

func getPrefixCmd(db *boltplus.DB) {
	if *bucketPath == "" {
		log.Fatal("specify bucket")
	}
	ch, err := db.GetPrefix(*bucketPath, *prefix)
	if err != nil {
		log.Fatal(err)
	}
	for val := range ch {
		data, err := json.Marshal(val)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(data))
	}
}

func getRangeCmd(db *boltplus.DB) {
	if *bucketPath == "" {
		log.Fatal("specify bucket")
	}
	ch, err := db.GetRange(*bucketPath, *rangeStart, *rangeEnd)
	if err != nil {
		log.Fatal(err)
	}
	for val := range ch {
		data, err := json.Marshal(val)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(data))
	}
}

func filterCmd(db *boltplus.DB) {
	if *bucketPath == "" {
		log.Fatal("specify bucket")
	}
	var ch chan *boltplus.Pair
	var err error
	if *all {
		ch, err = db.Find(*bucketPath, *filter)
	} else if *prefix != "" {
		ch, err = db.FindPrefix(*bucketPath, *prefix, *filter)
	} else if *rangeStart != "" && *rangeEnd != "" {
		ch, err = db.FindRange(*bucketPath, *rangeStart, *rangeEnd, *filter)
	} else {
		err = errors.New("please specify what to filter")
	}
	if err != nil {
		log.Fatal(err)
	}
	for val := range ch {
		data, err := json.Marshal(val)
		if err != nil {
			log.Fatal(err)
		}
		fmt.Println(string(data))
	}
}

func main() {
	flag.Parse()
	db, err := boltplus.New(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	if *put {
		putCmd(db)
	} else if *filter != "" {
		filterCmd(db)
	} else if *get {
		getCmd(db)
	} else if *delete {
		deleteCmd(db)
	} else if *all {
		getAllCmd(db)
	} else if *prefix != "" {
		getPrefixCmd(db)
	} else if *rangeStart != "" && *rangeEnd != "" {
		getRangeCmd(db)
	} else {
		log.Fatal("please specify what to do")
	}
}
