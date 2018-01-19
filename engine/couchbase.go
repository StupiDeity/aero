package engine

import (
	"encoding/json"
	"time"

	cb "github.com/couchbase/gocb"
	"github.com/stupideity/aero/key"
)

type Couchbase struct {
	key.AsIsFormat
	cb *cb.Bucket
}

func NewCouchbase(host string, port string, bucket string) Couchbase {

	c, err := cb.Connect("couchbase://" + host)
	if err != nil {
		panic(err)
	}

	b, err := c.OpenBucket(bucket, "")
	if err != nil {
		panic(err)
	}

	return Couchbase{
		cb: b,
	}
}

func (c Couchbase) Get(key string) ([]byte, error) {
	key = c.Format(key)

	var data interface{}
	var byteData []byte

	_, err := c.cb.Get(key, &data)
	if err == nil {
		byteData, err = json.Marshal(data)
	} else {
		return nil, err
	}

	return byteData, nil
}

func (c Couchbase) Set(key string, data []byte, expireIn time.Duration) {
	key = c.Format(key)

	c.cb.Upsert(key, data, uint32(expireIn))
}

func (c Couchbase) Close() {
	err := c.cb.Close()
	if err != nil {
		panic(err)
	}
}

func (c Couchbase) Delete(key string) error {
	key = c.Format(key)

	var data = make(map[string]interface{})

	cas, _ := c.cb.Get(key, &data)
	_, err := c.cb.Remove(key, cas)

	return err
}
