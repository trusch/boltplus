package boltplus

import (
	"os"
	"reflect"
	"strconv"
	"testing"
)

func setupCleanDB() (*DB, error) {
	os.Remove("./test.db")
	return New("./test.db")
}

func putN(db *DB, n int) error {
	tx, _ := db.Tx(true)
	defer tx.Close()
	for i := 0; i < n; i++ {
		err := tx.Put("test.bucket", strconv.Itoa(i), Object{"key": i})
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func TestNewWithValidPath(t *testing.T) {
	db, err := setupCleanDB()
	if err != nil {
		t.Error(err)
	}
	defer db.Close()
}

func TestNewWithInvalidPath(t *testing.T) {
	db, err := New("/invalid/path/test.db")
	if err == nil {
		t.Error("should fail")
		defer db.Close()
	}
}

func TestCreateReadTx(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	tx, err := db.Tx(false)
	if err != nil {
		t.Error(err)
	}
	defer tx.Close()
}

func TestCreateReadWriteTx(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	tx, err := db.Tx(true)
	if err != nil {
		t.Error(err)
	}
	defer tx.Close()
}

func TestPutGet(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	doc := Object{
		"foo": "bar",
		"baz": 23.,
	}
	if err := db.Put("test.bucket", "testkey", doc); err != nil {
		t.Error(err)
	}
	if result, err := db.Get("test.bucket", "testkey"); err != nil {
		t.Error(err)
	} else {
		if !reflect.DeepEqual(result, doc) {
			t.Errorf("wanted %v got %v", doc, result)
		}
	}
}

func TestGetInvalid(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	if _, err := db.Get("test.bucket", "testkey"); err == nil {
		t.Error("should fail to read unknown key")
	}
}

func TestPutPutGet(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	doc1 := Object{"a": 1.}
	if err := db.Put("test.bucket", "testkey", doc1); err != nil {
		t.Error(err)
	}
	doc2 := Object{"a": 2.}
	if err := db.Put("test.bucket", "testkey", doc2); err != nil {
		t.Error(err)
	}
	if result, err := db.Get("test.bucket", "testkey"); err != nil {
		t.Error(err)
	} else {
		if !reflect.DeepEqual(result, doc2) {
			t.Errorf("wanted %v got %v", doc2, result)
		}
	}
}

func TestPutN(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	if err := putN(db, 1000); err != nil {
		t.Error(err)
	}
	if result, err := db.Get("test.bucket", "999"); err != nil {
		t.Error(err)
	} else {
		expect := Object{"key": 999.}
		if !reflect.DeepEqual(result, expect) {
			t.Errorf("wanted %v got %v", expect, result)
		}
	}
}

func TestGetPrefix(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	putN(db, 11)
	if result, err := db.GetPrefix("test.bucket", "1"); err != nil {
		t.Error(err)
	} else {
		expect0 := &Pair{"1", Object{"key": 1.}}
		expect1 := &Pair{"10", Object{"key": 10.}}
		if doc := <-result; !reflect.DeepEqual(doc, expect0) {
			t.Errorf("wanted %v got %v", expect0, doc)
		}
		if doc := <-result; !reflect.DeepEqual(doc, expect1) {
			t.Errorf("wanted %v got %v", expect1, doc)
		}
	}
}

func TestGetRange(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	putN(db, 11)
	if result, err := db.GetRange("test.bucket", "1", "3"); err != nil {
		t.Error(err)
	} else {
		expect0 := &Pair{"1", Object{"key": 1.}}
		expect1 := &Pair{"10", Object{"key": 10.}}
		expect2 := &Pair{"2", Object{"key": 2.}}
		expect3 := &Pair{"3", Object{"key": 3.}}
		if doc := <-result; !reflect.DeepEqual(doc, expect0) {
			t.Errorf("wanted %v got %v", expect0, doc)
		}
		if doc := <-result; !reflect.DeepEqual(doc, expect1) {
			t.Errorf("wanted %v got %v", expect1, doc)
		}
		if doc := <-result; !reflect.DeepEqual(doc, expect2) {
			t.Errorf("wanted %v got %v", expect2, doc)
		}
		if doc := <-result; !reflect.DeepEqual(doc, expect3) {
			t.Errorf("wanted %v got %v", expect3, doc)
		}
	}
}

func TestGetAll(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	putN(db, 9)
	if result, err := db.GetAll("test.bucket"); err != nil {
		t.Error(err)
	} else {
		for i := 0; i < 9; i++ {
			expect := &Pair{strconv.Itoa(i), Object{"key": float64(i)}}
			if doc := <-result; !reflect.DeepEqual(doc, expect) {
				t.Errorf("wanted %v got %v", expect, doc)
			}
		}
	}
}

func TestDelete(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	putN(db, 1)
	if err := db.Delete("test.bucket", "1"); err != nil {
		t.Error(err)
	}
	if _, err := db.Get("test.bucket", "1"); err == nil {
		t.Error("Get after Delete should fail")
	}
}

func TestBuckets(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	db.Put("foo.bar.a", "key", Object{})
	db.Put("foo.bar.b", "key", Object{})
	db.Put("foo.bar.c", "key", Object{})
	if buckets, err := db.Buckets(); err != nil {
		t.Error(err)
	} else {
		expect := []string{
			"foo",
			"foo.bar",
			"foo.bar.a",
			"foo.bar.b",
			"foo.bar.c",
		}
		if !reflect.DeepEqual(buckets, expect) {
			t.Errorf("wanted %v got %v", expect, buckets)
		}
	}
}

func TestBackup(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	putN(db, 4)
	f, err := os.Create("./backup.db")
	if err != nil {
		t.Error("create backup file: ", err)
	}
	defer f.Close()
	defer os.Remove("./backup.db")
	if err = db.Backup(f); err != nil {
		t.Error("perform backup: ", err)
	}
	f.Close()
	backup, err := New("./backup.db")
	if err != nil {
		t.Error("opening backup db: ", err)
	}
	defer backup.Close()
	if result, err := backup.GetAll("test.bucket"); err != nil {
		t.Error(err)
	} else {
		expect0 := &Pair{"0", Object{"key": 0.}}
		expect1 := &Pair{"1", Object{"key": 1.}}
		expect2 := &Pair{"2", Object{"key": 2.}}
		expect3 := &Pair{"3", Object{"key": 3.}}
		if doc := <-result; !reflect.DeepEqual(doc, expect0) {
			t.Errorf("wanted %v got %v", expect0, doc)
		}
		if doc := <-result; !reflect.DeepEqual(doc, expect1) {
			t.Errorf("wanted %v got %v", expect1, doc)
		}
		if doc := <-result; !reflect.DeepEqual(doc, expect2) {
			t.Errorf("wanted %v got %v", expect2, doc)
		}
		if doc := <-result; !reflect.DeepEqual(doc, expect3) {
			t.Errorf("wanted %v got %v", expect3, doc)
		}
	}
}

func TestFind(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	putN(db, 100)
	if result, err := db.Find("test.bucket", ".key >= 98"); err != nil {
		t.Error(err)
	} else {
		expect0 := &Pair{"98", Object{"key": 98.}}
		expect1 := &Pair{"99", Object{"key": 99.}}
		if doc := <-result; !reflect.DeepEqual(expect0, doc) {
			t.Errorf("wanted %v got %v", expect0, doc)
		}
		if doc := <-result; !reflect.DeepEqual(expect1, doc) {
			t.Errorf("wanted %v got %v", expect0, doc)
		}
	}
}

func TestFindRange(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	putN(db, 100)
	if result, err := db.FindRange("test.bucket", "50", "99", ".key >= 98"); err != nil {
		t.Error(err)
	} else {
		expect0 := &Pair{"98", Object{"key": 98.}}
		expect1 := &Pair{"99", Object{"key": 99.}}
		if doc := <-result; !reflect.DeepEqual(expect0, doc) {
			t.Errorf("wanted %v got %v", expect0, doc)
		}
		if doc := <-result; !reflect.DeepEqual(expect1, doc) {
			t.Errorf("wanted %v got %v", expect0, doc)
		}
	}
}

func TestFindPrefix(t *testing.T) {
	db, _ := setupCleanDB()
	defer db.Close()
	putN(db, 100)
	if result, err := db.FindPrefix("test.bucket", "9", ".key >= 98"); err != nil {
		t.Error(err)
	} else {
		expect0 := &Pair{"98", Object{"key": 98.}}
		expect1 := &Pair{"99", Object{"key": 99.}}
		if doc := <-result; !reflect.DeepEqual(expect0, doc) {
			t.Errorf("wanted %v got %v", expect0, doc)
		}
		if doc := <-result; !reflect.DeepEqual(expect1, doc) {
			t.Errorf("wanted %v got %v", expect0, doc)
		}
	}
}
