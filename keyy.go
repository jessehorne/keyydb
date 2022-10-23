package keyy

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"os"
)

const TYPE_INT32 = 0
const TYPE_INT64 = 1
const TYPE_STRING = 2
const TYPE_FLOAT32 = 3
const TYPE_FLOAT64 = 4

const KEY_SIZE = 59         // 50 bytes for key string, 1 byte for value type, 4 bytes for ref (the part of the byte array the value starts), 4 bytes for value size (e.g 3 bytes for "cat" or 4 bytes for the number 42)
const KEY_MAX_KEY_SIZE = 50 // a key can only be up to 50 bytes long

type Var struct {
	T uint8  // type
	R uint32 // reference
	S uint32 // size
	V []byte
}

type DB struct {
	Path      string
	Size      int64
	Keys      map[string]*Var
	KeysCount uint32
}

func Open(path string) (*DB, error) {
	newDB := DB{
		Path:      path,
		Size:      0,
		Keys:      map[string]*Var{},
		KeysCount: 0,
	}

	// local file, create if not exists
	fileBytes, err := os.ReadFile(path)
	if err != nil {
		_, err = os.Create(path)
		if err != nil {
			return nil, err
		}
	}

	fileByteCount := len(fileBytes)
	if fileByteCount == 0 {
		return &newDB, nil
	}

	newDB.Size = int64(fileByteCount)

	keysCount := binary.BigEndian.Uint32(fileBytes[0:4])
	startAddress := uint32(4) // key rows start after the 4 byte key count field in the file
	endAddress := startAddress + (keysCount * KEY_SIZE)
	refStart := 4 + keysCount*KEY_SIZE
	for i := startAddress; i < endAddress; i += KEY_SIZE {
		// read key (50 bytes)
		keyBuf := fileBytes[i : i+50]
		key := string(bytes.Trim(keyBuf, "\x00")) // remember key is 50 bytes

		varType := fileBytes[i+50]

		// read []byte index (4 bytes)
		ref := binary.BigEndian.Uint32(fileBytes[i+51 : i+55]) // []byte index reference is 4 bytes

		// read next 4 bytes that is a uint32 telling how big the value is the key points to
		valueSize := binary.BigEndian.Uint32(fileBytes[i+55 : i+59]) // value size (number of bytes of value itself) is 4

		newDB.Keys[key] = &Var{
			T: varType,
			R: ref,
			S: valueSize,
			V: nil,
		}

		// store value
		if varType == TYPE_INT32 {
			if valueSize != 4 {
				continue
			}
			newDB.Keys[key].V = fileBytes[refStart+ref : refStart+ref+4]
		} else if varType == TYPE_INT64 {
			if valueSize != 8 {
				continue
			}
			newDB.Keys[key].V = fileBytes[refStart+ref : refStart+ref+8]
		} else if varType == TYPE_STRING {
			newDB.Keys[key].V = fileBytes[refStart+ref : refStart+ref+valueSize]
		} else if varType == TYPE_FLOAT32 {
			if valueSize != 4 {
				continue
			}
			newDB.Keys[key].V = fileBytes[refStart+ref : refStart+ref+4]
		} else if varType == TYPE_FLOAT64 {
			if valueSize != 8 {
				continue
			}
			newDB.Keys[key].V = fileBytes[refStart+ref : refStart+ref+8]
		}
	}

	newDB.KeysCount = keysCount

	return &newDB, nil
}

func (db *DB) Sync() error {
	// local file, create if not exists
	f, err := os.Create(db.Path)
	if err != nil {
		fmt.Println("errored at OpenFile")
		return err
	}

	keysCount := uint32(len(db.Keys))

	// key count header
	keyCountHeader := make([]byte, 4)
	binary.BigEndian.PutUint32(keyCountHeader, keysCount)

	// create keys buffer to be stored later
	var keys []byte

	// create values buffer to be stored later
	var values []byte
	var valuesLen uint32 = 0

	// loop through all key/values and store in keys and values as appropriate
	for key, value := range db.Keys {
		var keyBytes []byte // to be added to keys later

		// add 50 byte key header to keyBytes
		keyLength := len(key)
		if keyLength == 0 {
			return errors.New(fmt.Sprintf("KEY %s cannot be a 0 length string", key))
		} else if keyLength > 0 && keyLength <= 50 {
			// add padding to make key itself 50 bytes
			keyBytes = append(keyBytes, []byte(key)...)
			if keyLength < 50 {
				keyBytes = append(keyBytes, make([]byte, KEY_MAX_KEY_SIZE-keyLength)...)
			}
		} else {
			return errors.New(fmt.Sprintf("KEY %s cannot be more than %v characters in length", key, KEY_MAX_KEY_SIZE))
		}

		// add 1 byte type
		keyBytes = append(keyBytes, value.T)

		// add 4 byte ref
		ref := make([]byte, 4)
		binary.BigEndian.PutUint32(ref, valuesLen)
		keyBytes = append(keyBytes, ref...)

		// add 4 byte size
		vSize := make([]byte, 4)
		binary.BigEndian.PutUint32(vSize, value.S)
		keyBytes = append(keyBytes, vSize...)

		// add the current key header row to the keys byte array
		keys = append(keys, keyBytes...)

		// add the value itself to the valueBytes array
		values = append(values, value.V...)

		if value.T == TYPE_INT32 {
			valuesLen += uint32(4)
		} else if value.T == TYPE_INT64 {
			valuesLen += uint32(8)
		} else if value.T == TYPE_FLOAT32 {
			valuesLen += uint32(4)
		} else if value.T == TYPE_FLOAT64 {
			valuesLen += uint32(8)
		} else if value.T == TYPE_STRING {
			valuesLen += uint32(len(value.V))
		}
	}

	// DEBUG
	//fmt.Println("Keys Count: ", keysCount)
	//fmt.Println("Header Size: ", len(keyCountHeader))
	//fmt.Println("Keys Size: ", len(keys))
	//fmt.Println("Values Size: ", len(values))

	_, err = f.Write(keyCountHeader)
	if err != nil {
		fmt.Println("errored while storing key count header")
		return err
	}

	_, err = f.Write(keys)
	if err != nil {
		fmt.Println("errored at first write")
		return err
	}

	_, err = f.Write(values)
	if err != nil {
		fmt.Println("errored at second write")
		return err
	}

	err = f.Close()
	if err != nil {
		fmt.Println("errored at close")
		return err
	}

	return nil
}

func (db *DB) Set(key string, value interface{}) error {
	var valueBuf []byte
	var valueType uint8
	var valueSize uint32

	switch value.(type) {
	case int32:
		valueType = TYPE_INT32
		valueSize = 4
		valueBuf = make([]byte, 4)
		binary.BigEndian.PutUint32(valueBuf, uint32(value.(int32)))
	case int64:
		valueType = TYPE_INT64
		valueSize = 8
		valueBuf = make([]byte, 8)
		binary.BigEndian.PutUint64(valueBuf, uint64(value.(int64)))
	case float32:
		valueType = TYPE_FLOAT32
		valueSize = 4
		valueBuf = make([]byte, 4)
		binary.BigEndian.PutUint32(valueBuf, math.Float32bits(value.(float32)))
	case float64:
		valueType = TYPE_FLOAT64
		valueSize = 8
		valueBuf = make([]byte, 8)
		binary.BigEndian.PutUint64(valueBuf, math.Float64bits(value.(float64)))
	case string:
		valueType = TYPE_STRING
		valueSize = uint32(len(value.(string)))
		valueBuf = []byte(value.(string))
	}

	k, exists := db.Keys[key]
	if exists {
		k.T = valueType
		k.R = 0
		k.S = valueSize
		k.V = valueBuf
	} else {
		newVar := Var{
			T: valueType,
			R: 0,
			S: valueSize,
			V: valueBuf,
		}

		db.Keys[key] = &newVar
	}

	return nil
}

func (db *DB) Get(key string) (interface{}, error) {
	v, exists := db.Keys[key]
	if !exists {
		return nil, errors.New("key doesn't exist")
	}

	switch v.T {
	case TYPE_INT32:
		return int32(binary.BigEndian.Uint32(v.V)), nil
	case TYPE_INT64:
		return int64(binary.BigEndian.Uint64(v.V)), nil
	case TYPE_FLOAT32:
		return math.Float32frombits(binary.BigEndian.Uint32(v.V)), nil
	case TYPE_FLOAT64:
		return math.Float64frombits(binary.BigEndian.Uint64(v.V)), nil
	case TYPE_STRING:
		return string(v.V), nil
	default:
		return nil, errors.New("unsupported type")
	}

	return nil, errors.New("this shouldn't happen")
}
