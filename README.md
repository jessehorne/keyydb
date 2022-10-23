keyydb
===

A very simple and embeddable key value store for Go.

**WARNING: You shouldn't use this for anything important!**

# Installation
```sh
go get github.com/jessehorne/keyydb
```


# Usage

There are currently 5 supported types of data that you can store.

1. int32
2. int64
3. float32
4. float64
5. string

You should keep track of the data you're storing and make sure they match this type.

## Open a DB or Create If Not Exists
```go
db, err := keyy.Open("./test.keyy")
```

## Store a value
```go
err := db.Set("name", "Jesse")
```

## Get a value
```go
myName, err := db.Get("name")
fmt.Println(myName.(string))
```

## Persist the key value memory store to disk
```go
err := db.Sync()
```

more coming soon...

# LICENSE

See `./LICENSE`.