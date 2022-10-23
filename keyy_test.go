package keyy_test

import (
	"errors"
	"fmt"
	keyy "github.com/jessehorne/keyydb"
	"os"
	"testing"
)

func TestKeyy_CreateDBIfNotExists(t *testing.T) {
	path := "/tmp/shouldNotExist.keyy"

	db, err := keyy.Open(path)
	if err != nil {
		t.Error("Something went wrong while opening file that shouldn't exist...", err)
	}
	if db == nil {
		t.Error("db shouldn't be nil here")
	}

	// test that file exists
	if _, err := os.Stat(path); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			t.Error("The test database should have been created but wasn't...")
		}
	}

	err = os.Remove(path)
	if err != nil {
		t.Error(fmt.Sprintf("couldn't remove '%s', you will have to do this manually...", path))
	}
}

func TestKeyy_FullTest(t *testing.T) {
	path := "/tmp/shouldNotExist.keyy"
	db, err := keyy.Open(path)
	if err != nil {
		t.Error("something went wrong while opening non-existent database", err)
	}

	// set a few values
	err = db.Set("test-string", "Jesse")
	if err != nil {
		t.Error("something went wrong while setting value for 'string'", err)
	}

	err = db.Set("test-int32", int32(10))
	if err != nil {
		t.Error("something went wrong while setting value for 'test-int32'", err)
	}

	err = db.Set("test-int64", int64(666))
	if err != nil {
		t.Error("something went wrong while setting value for 'test-int64'", err)
	}

	err = db.Set("test-float32", float32(42.42))
	if err != nil {
		t.Error("something went wrong while setting value for 'test-float32'", err)
	}

	err = db.Set("test-float64", float64(666.666))
	if err != nil {
		t.Error("something went wrong while setting value for 'test-float64'", err)
	}

	// sync to file
	err = db.Sync()
	if err != nil {
		t.Error("something went wrong while syncing to disk", err)
	}

	// Now reload the db from file
	db, err = keyy.Open(path)
	if err != nil {
		t.Error("something went wrong while re-loading database from file", err)
	}

	// get values
	if _, err := db.Get("test-int32"); err != nil {
		t.Error("something went wrong while grabbing test-int32", err)
	}

	if _, err := db.Get("test-int64"); err != nil {
		t.Error("something went wrong while grabbing test-int64", err)
	}

	if _, err := db.Get("test-float32"); err != nil {
		t.Error("something went wrong while grabbing test-float32", err)
	}

	if _, err := db.Get("test-float64"); err != nil {
		t.Error("something went wrong while grabbing test-float64", err)
	}

	if _, err := db.Get("test-string"); err != nil {
		t.Error("something went wrong while grabbing test-string", err)
	}

	// TODO: write tests to compare values...floats are messed up right now kinda (e.g 6.66 might return something like 6.555559)
}
