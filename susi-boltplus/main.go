package main

import (
	"flag"
	"log"

	"github.com/trusch/boltplus"
	"github.com/webvariants/susigo"
)

var addr = flag.String("addr", ":4000", "susi server address")
var key = flag.String("key", "key.key", "susi key")
var cert = flag.String("cert", "cert.crt", "susi cert")

var dbPath = flag.String("db", "/usr/share/susi/boltplus.db", "db path")

var db *boltplus.DB

func init() {
	flag.Parse()
	d, err := boltplus.New(*dbPath)
	if err != nil {
		log.Fatal(err)
	}
	db = d
}

func main() {
	susi, err := susigo.NewSusi(*addr, *cert, *key)
	if err != nil {
		log.Fatal(err)
	}
	susi.Wait()
	susi.RegisterProcessor("^boltplus::put$", handlePut)
	susi.RegisterProcessor("^boltplus::get$", handleGet)
	susi.RegisterProcessor("^boltplus::delete$", handleDelete)
	susi.RegisterProcessor("^boltplus::getAll$", handleGetAll)
	susi.RegisterProcessor("^boltplus::getPrefix$", handleGetPrefix)
	susi.RegisterProcessor("^boltplus::getRange$", handleGetRange)
	susi.RegisterProcessor("^boltplus::find$", handleFind)
	susi.RegisterProcessor("^boltplus::findPrefix$", handleFindPrefix)
	susi.RegisterProcessor("^boltplus::findRange$", handleFindRange)

	select {}
}

func handlePut(event *susigo.Event) {
	var (
		wrongPayloadError = "payload must be object with 'bucket', 'key' and 'doc'"
		bucket            string
		key               string
		doc               map[string]interface{}
	)
	if payload, ok := event.Payload.(map[string]interface{}); ok {
		if bucket, ok = payload["bucket"].(string); ok {
			if key, ok = payload["key"].(string); ok {
				if doc, ok = payload["doc"].(map[string]interface{}); ok {
					err := db.Put(bucket, key, doc)
					if err != nil {
						event.AddHeader("Error", err.Error())
						event.Dismiss()
					} else {
						payload["success"] = true
						event.Ack()
					}
				}
			}
		}
	}
	if bucket == "" || key == "" || doc == nil {
		event.AddHeader("Error", wrongPayloadError)
		event.Dismiss()
		return
	}
}

func handleGet(event *susigo.Event) {
	var (
		wrongPayloadError = "payload must be object with 'bucket' and 'key'"
		bucket            string
		key               string
	)
	if payload, ok := event.Payload.(map[string]interface{}); ok {
		if bucket, ok = payload["bucket"].(string); ok {
			if key, ok = payload["key"].(string); ok {
				doc, err := db.Get(bucket, key)
				if err != nil {
					event.AddHeader("Error", err.Error())
					event.Dismiss()
				} else {
					payload["doc"] = doc
					event.Ack()
				}
			}
		}
	}
	if bucket == "" || key == "" {
		event.AddHeader("Error", wrongPayloadError)
		event.Dismiss()
		return
	}
}

func handleDelete(event *susigo.Event) {
	var (
		wrongPayloadError = "payload must be object with 'bucket' and 'key'"
		bucket            string
		key               string
	)
	if payload, ok := event.Payload.(map[string]interface{}); ok {
		if bucket, ok = payload["bucket"].(string); ok {
			if key, ok = payload["key"].(string); ok {
				err := db.Delete(bucket, key)
				if err != nil {
					event.AddHeader("Error", err.Error())
					event.Dismiss()
				} else {
					payload["success"] = true
					event.Ack()
				}
			}
		}
	}
	if bucket == "" || key == "" {
		event.AddHeader("Error", wrongPayloadError)
		event.Dismiss()
		return
	}
}

func handleGetRange(event *susigo.Event) {
	var (
		wrongPayloadError = "payload must be object with 'bucket','start' and 'end'"
		bucket            string
		start             string
		end               string
	)
	if payload, ok := event.Payload.(map[string]interface{}); ok {
		if bucket, ok = payload["bucket"].(string); ok {
			if start, ok = payload["start"].(string); ok {
				if end, ok = payload["end"].(string); ok {
					ch, err := db.GetRange(bucket, start, end)
					if err != nil {
						event.AddHeader("Error", err.Error())
						event.Dismiss()
					} else {
						arr := make([]*boltplus.Pair, 0, 64)
						for pair := range ch {
							arr = append(arr, pair)
						}
						payload["docs"] = arr
						event.Ack()
					}
				}
			}
		}
	}
	if bucket == "" || start == "" || end == "" {
		event.AddHeader("Error", wrongPayloadError)
		event.Dismiss()
		return
	}
}

func handleGetPrefix(event *susigo.Event) {
	var (
		wrongPayloadError = "payload must be object with 'bucket' and 'prefix'"
		bucket            string
		prefix            string
	)
	if payload, ok := event.Payload.(map[string]interface{}); ok {
		if bucket, ok = payload["bucket"].(string); ok {
			if prefix, ok = payload["prefix"].(string); ok {
				ch, err := db.GetPrefix(bucket, prefix)
				if err != nil {
					event.AddHeader("Error", err.Error())
					event.Dismiss()
				} else {
					arr := make([]*boltplus.Pair, 0, 64)
					for pair := range ch {
						arr = append(arr, pair)
					}
					payload["docs"] = arr
					event.Ack()
				}
			}
		}
	}
	if bucket == "" || prefix == "" {
		event.AddHeader("Error", wrongPayloadError)
		event.Dismiss()
		return
	}
}

func handleGetAll(event *susigo.Event) {
	var (
		wrongPayloadError = "payload must be object with 'bucket'"
		bucket            string
	)
	if payload, ok := event.Payload.(map[string]interface{}); ok {
		if bucket, ok = payload["bucket"].(string); ok {
			ch, err := db.GetAll(bucket)
			if err != nil {
				event.AddHeader("Error", err.Error())
				event.Dismiss()
			} else {
				arr := make([]*boltplus.Pair, 0, 64)
				for pair := range ch {
					arr = append(arr, pair)
				}
				payload["docs"] = arr
				event.Ack()
			}
		}
	}
	if bucket == "" {
		event.AddHeader("Error", wrongPayloadError)
		event.Dismiss()
		return
	}
}

func handleFind(event *susigo.Event) {
	var (
		wrongPayloadError = "payload must be object with 'bucket' and 'filter'"
		bucket            string
		filter            string
	)
	if payload, ok := event.Payload.(map[string]interface{}); ok {
		if bucket, ok = payload["bucket"].(string); ok {
			if filter, ok = payload["filter"].(string); ok {
				ch, err := db.Find(bucket, filter)
				if err != nil {
					event.AddHeader("Error", err.Error())
					event.Dismiss()
				} else {
					arr := make([]*boltplus.Pair, 0, 64)
					for pair := range ch {
						arr = append(arr, pair)
					}
					payload["docs"] = arr
					event.Ack()
				}
			}
		}
	}
	if bucket == "" || filter == "" {
		event.AddHeader("Error", wrongPayloadError)
		event.Dismiss()
		return
	}
}

func handleFindRange(event *susigo.Event) {
	var (
		wrongPayloadError = "payload must be object with 'bucket', 'start', 'end' and 'filter'"
		bucket            string
		filter            string
		start             string
		end               string
	)
	if payload, ok := event.Payload.(map[string]interface{}); ok {
		if bucket, ok = payload["bucket"].(string); ok {
			if filter, ok = payload["filter"].(string); ok {
				if start, ok = payload["start"].(string); ok {
					if end, ok = payload["end"].(string); ok {
						ch, err := db.FindRange(bucket, start, end, filter)
						if err != nil {
							event.AddHeader("Error", err.Error())
							event.Dismiss()
						} else {
							arr := make([]*boltplus.Pair, 0, 64)
							for pair := range ch {
								arr = append(arr, pair)
							}
							payload["docs"] = arr
							event.Ack()
						}
					}
				}
			}
		}
	}
	if bucket == "" || filter == "" || start == "" || end == "" {
		event.AddHeader("Error", wrongPayloadError)
		event.Dismiss()
		return
	}
}

func handleFindPrefix(event *susigo.Event) {
	var (
		wrongPayloadError = "payload must be object with 'bucket', 'prefix' and 'filter'"
		bucket            string
		filter            string
		prefix            string
	)
	if payload, ok := event.Payload.(map[string]interface{}); ok {
		if bucket, ok = payload["bucket"].(string); ok {
			if filter, ok = payload["filter"].(string); ok {
				if prefix, ok = payload["prefix"].(string); ok {
					ch, err := db.FindPrefix(bucket, prefix, filter)
					if err != nil {
						event.AddHeader("Error", err.Error())
						event.Dismiss()
					} else {
						arr := make([]*boltplus.Pair, 0, 64)
						for pair := range ch {
							arr = append(arr, pair)
						}
						payload["docs"] = arr
						event.Ack()
					}
				}
			}
		}
	}
	if bucket == "" || filter == "" || prefix == "" {
		event.AddHeader("Error", wrongPayloadError)
		event.Dismiss()
		return
	}
}
