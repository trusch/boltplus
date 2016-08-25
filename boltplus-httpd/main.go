package main

import (
	"encoding/json"
	"log"
	"net/http"
	"strings"

	"github.com/trusch/boltplus"
)

var db *boltplus.DB

func init() {
	d, err := boltplus.New("default.db")
	if err != nil {
		log.Fatal(err)
	}
	db = d
}

// URL schema:
// PUT GET DELETE /foo/bar/baz
//   -> use doc with key baz in bucket foo.bar for single doc manipulation
// GET /prefix?bucket=foo.bar&prefix=baz
//   -> get all docs with key prefix baz in bucket foo.bar
// GET /range?bucket=foo.bar&start=baz&end=qux
//   -> get all docs with key in range baz-qux in bucket foo.bar
// GET /find?bucket=foo.bar&filter=".a == 'foo'"
//   -> get all docs with key a equal foo in bucket foo.bar
// GET /findPrefix?bucket=foo.bar&filter=".a == 'foo'"&prefix=baz
//   -> get all docs with key a equal foo in bucket foo.bar
// GET /findRange?bucket=foo.bar&filter=".a == 'foo'"&start=baz&end=qux
//   -> get all docs with key a equal foo in bucket foo.bar
func defaultHandler(w http.ResponseWriter, req *http.Request) {
	if req.URL.String() == "/favicon.ico" {
		http.NotFound(w, req)
		return
	}
	parts := strings.Split(strings.Trim(req.URL.Path, "/"), "/")
	query := req.URL.Query()
	log.Print(parts)
	switch parts[0] {
	case "all":
		{
			handleAll(query.Get("bucket"), w)
		}
	case "prefix":
		{
			handlePrefix(query.Get("bucket"), query.Get("prefix"), w)
		}
	case "range":
		{
			handleRange(query.Get("bucket"), query.Get("start"), query.Get("end"), w)
		}
	case "find":
		{
			handleFind(query.Get("bucket"), query.Get("filter"), w)
		}
	case "findPrefix":
		{
			handleFindPrefix(query.Get("bucket"), query.Get("prefix"), query.Get("filter"), w)
		}
	case "findRange":
		{
			handleFindRange(query.Get("bucket"), query.Get("start"), query.Get("end"), query.Get("filter"), w)
		}
	default:
		{
			if len(parts) < 2 {
				http.Error(w, "malformed request", http.StatusBadRequest)
				return
			}
			handleDefaultRequest(strings.Join(parts[0:len(parts)-1], "."), parts[len(parts)-1], req, w)
		}
	}
}

func handleDefaultRequest(bucket, key string, req *http.Request, w http.ResponseWriter) {
	switch req.Method {
	case http.MethodGet:
		{
			doc, err := db.Get(bucket, key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			bs, _ := json.Marshal(doc)
			w.Header().Set("Content-Type", "application/json")
			w.Write(bs)
		}
	case http.MethodDelete:
		{
			err := db.Delete(bucket, key)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	case http.MethodPost:
		fallthrough
	case http.MethodPatch:
		fallthrough
	case http.MethodPut:
		{
			var doc map[string]interface{}
			decoder := json.NewDecoder(req.Body)
			err := decoder.Decode(&doc)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			err = db.Put(bucket, key, doc)
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			w.WriteHeader(http.StatusOK)
		}
	}
}

func handlePrefix(bucket, prefix string, w http.ResponseWriter) {
	ch, err := db.GetPrefix(bucket, prefix)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	log.Print("got channel")
	res := make([]*boltplus.Pair, 0, 64)
	for pair := range ch {
		log.Print(pair)
		res = append(res, pair)
	}
	log.Print("ready")
	bs, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func handleRange(bucket, start, end string, w http.ResponseWriter) {
	ch, err := db.GetRange(bucket, start, end)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	res := make([]*boltplus.Pair, 0, 64)
	for pair := range ch {
		res = append(res, pair)
	}
	bs, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func handleAll(bucket string, w http.ResponseWriter) {
	log.Print("bucket: ", []byte(bucket))
	ch, err := db.GetAll(bucket)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	res := make([]*boltplus.Pair, 0, 64)
	for pair := range ch {
		res = append(res, pair)
	}
	bs, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func handleFind(bucket, filter string, w http.ResponseWriter) {
	log.Print("bucket: ", []byte(bucket))
	ch, err := db.Find(bucket, filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	res := make([]*boltplus.Pair, 0, 64)
	for pair := range ch {
		res = append(res, pair)
	}
	bs, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func handleFindRange(bucket, start, end, filter string, w http.ResponseWriter) {
	log.Print("bucket: ", []byte(bucket))
	ch, err := db.FindRange(bucket, start, end, filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	res := make([]*boltplus.Pair, 0, 64)
	for pair := range ch {
		res = append(res, pair)
	}
	bs, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func handleFindPrefix(bucket, prefix, filter string, w http.ResponseWriter) {
	log.Print("bucket: ", []byte(bucket))
	ch, err := db.FindPrefix(bucket, prefix, filter)
	if err != nil {
		http.Error(w, err.Error(), http.StatusNotFound)
		return
	}
	res := make([]*boltplus.Pair, 0, 64)
	for pair := range ch {
		res = append(res, pair)
	}
	bs, _ := json.Marshal(res)
	w.Header().Set("Content-Type", "application/json")
	w.Write(bs)
}

func main() {
	http.HandleFunc("/", defaultHandler)
	log.Fatal(http.ListenAndServe(":8080", nil))
}
