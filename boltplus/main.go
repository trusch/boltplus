package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"

	"gopkg.in/yaml.v2"

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
var start = flag.String("start", "", "start to search")
var end = flag.String("end", "", "end to search")

var filter = flag.String("filter", "", "filter returned docs with gojee")
var backup = flag.String("backup", "", "backup the database to this file")
var buckets = flag.Bool("buckets", false, "list all buckets")

var outputFormat = flag.String("format", "json", "output format (json,json-pretty,yaml)")

func print(data interface{}) {
	var bs []byte
	switch *outputFormat {
	case "json":
		{
			bs, _ = json.Marshal(data)
		}
	case "json-pretty":
		{
			bs, _ = json.MarshalIndent(data, "", "  ")
		}
	case "yaml":
		{
			bs, _ = yaml.Marshal(data)
		}
	default:
		{
			log.Print("wrong output format")
			return
		}
	}
	fmt.Println(string(bs))
}

func init() {
	flag.Parse()
	if !*all && !*put && !*get && !*delete && *prefix == "" && *start == "" && *end == "" {
		if *bucketPath != "" && *key != "" && *doc != "" {
			*put = true
		} else if *bucketPath != "" && *key != "" {
			*get = true
		} else if *bucketPath != "" {
			*all = true
		} else if (*backup == "") && !*buckets {
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
	print(val)
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
	ch, err := db.GetAll(*bucketPath)
	if err != nil {
		log.Fatal(err)
	}
	for val := range ch {
		print(val)
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
		print(val)
	}
}

func getRangeCmd(db *boltplus.DB) {
	if *bucketPath == "" {
		log.Fatal("specify bucket")
	}
	ch, err := db.GetRange(*bucketPath, *start, *end)
	if err != nil {
		log.Fatal(err)
	}
	for val := range ch {
		print(val)
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
	} else if *start != "" && *end != "" {
		ch, err = db.FindRange(*bucketPath, *start, *end, *filter)
	} else {
		err = errors.New("please specify what to filter")
	}
	if err != nil {
		log.Fatal(err)
	}
	for val := range ch {
		print(val)
	}
}

func backupCmd(db *boltplus.DB) {
	f, err := os.Create(*backup)
	if err != nil {
		log.Fatal(err)
	}
	if err := db.Backup(f); err != nil {
		log.Fatal(err)
	}
	log.Printf("successfully created backup %v", *backup)
}

func bucketsCmd(db *boltplus.DB) {
	if list, err := db.Buckets(); err == nil {
		print(list)
	} else {
		log.Fatal(err)
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
	} else if *start != "" && *end != "" {
		getRangeCmd(db)
	} else if *backup != "" {
		backupCmd(db)
	} else if *buckets {
		bucketsCmd(db)
	} else {
		log.Fatal("please specify what to do")
	}
}
