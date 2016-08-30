package boltplus

import (
	"strconv"
	"testing"
)

func benchmarkPutN(num int, b *testing.B) {
	db, err := setupCleanDB()
	if err != nil {
		b.Error(err)
	}
	defer db.Close()
	for n := 0; n < b.N; n++ {
		for i := 0; i < num; i++ {
			err := db.Put("foo", strconv.Itoa(i), Object{"a": 1})
			if err != nil {
				b.Error(err)
			}
		}
	}
}

func benchmarkPutNTx(num int, b *testing.B) {
	db, err := setupCleanDB()
	if err != nil {
		b.Error(err)
	}
	defer db.Close()
	for n := 0; n < b.N; n++ {
		tx, err := db.Tx(true)
		if err != nil {
			b.Error(err)
		}
		for i := 0; i < num; i++ {
			e := tx.Put("foo", strconv.Itoa(i), Object{"a": 1})
			if e != nil {
				b.Error(err)
			}
		}
		err = tx.Commit()
		if err != nil {
			b.Error(err)
		}
	}
}

func BenchmarkPut100(b *testing.B)  { benchmarkPutN(100, b) }
func BenchmarkPut1000(b *testing.B) { benchmarkPutN(1000, b) }

func BenchmarkPut100Tx(b *testing.B)    { benchmarkPutNTx(100, b) }
func BenchmarkPut1000Tx(b *testing.B)   { benchmarkPutNTx(1000, b) }
func BenchmarkPut10000Tx(b *testing.B)  { benchmarkPutNTx(10000, b) }
func BenchmarkPut100000Tx(b *testing.B) { benchmarkPutNTx(100000, b) }
