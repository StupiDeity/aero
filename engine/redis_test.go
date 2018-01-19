package engine

import (
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

var redis_host string = "dockerhost"
var redis_port int = 6379
var redis_db int = 0
var redis_que string = "abcdef"

func TestRedisGetSet(t *testing.T) {

	r := NewRedis(redis_host, redis_port, redis_db)

	Convey("Given a Redis cache", t, func() {
		Convey("When you Set a key-value", func() {
			r.Set("i-am-the-key", []byte("stringy"), time.Minute*5)
			Convey("Then Get should give the same value back", func() {
				v, err := r.Get("i-am-the-key")
				So(err, ShouldBeNil)
				So(string(v), ShouldEqual, "stringy")
			})
		})
	})
}

func TestRedis_QuePushPop(t *testing.T) {

	emptyTheQue()
	r := NewRedis2(redis_host, redis_port, redis_db, redis_que)

	Convey("Given a Redis queue", t, func() {
		Convey("When you push two elements", func() {
			r.Push([]byte("one"))
			r.Push([]byte("two"))
			Convey("Then the lenght must be 2", func() {
				i, err := r.Len()
				So(err, ShouldBeNil)
				So(i, ShouldEqual, 2)
				Convey("And when you pop one value", func() {
					v, e1 := r.Pop()
					Convey("Then value should match and length must be 1", func() {
						So(e1, ShouldBeNil)
						So(string(v), ShouldEqual, "one")
						l, e2 := r.Len()
						So(e2, ShouldBeNil)
						So(l, ShouldEqual, 1)
						Convey("And when you pop another value", func() {
							v2, e3 := r.Pop()
							Convey("Then the value must match and length must be 0", func() {
								So(e3, ShouldBeNil)
								So(string(v2), ShouldEqual, "two")
								l2, e4 := r.Len()
								So(e4, ShouldBeNil)
								So(l2, ShouldEqual, 0)
							})
						})
					})
				})
			})
		})
	})

}

func emptyTheQue() {
	r := NewRedis2(redis_host, redis_port, redis_db, redis_que)
	for {
		i, err := r.Len()
		if err != nil {
			panic(err)
		}
		if i != 0 {
			r.Pop()
		} else {
			break
		}
	}
	r.Close()
}
