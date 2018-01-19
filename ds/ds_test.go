package ds

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func TestMapEncodeDecode(t *testing.T) {

	m := map[string]interface{}{"abc": "def", "lmn": 123}

	Convey("When you encode a map", t, func() {
		b, err := ToBytes(m, false)
		So(err, ShouldBeNil)
		Convey("And then decode it", func() {
			Convey("Then the values should match", func() {
				var d map[string]interface{}
				err := Load(&d, b)
				So(err, ShouldBeNil)
				So(m["abc"], ShouldEqual, d["abc"])
				So(m["lmn"], ShouldEqual, d["lmn"])
			})
		})
	})
}

type AStruct struct {
	MyString string
	MyInt    int
	MyJson   string `json:"my_json"`
}

func TestLoadStructByMap(t *testing.T) {

	var a AStruct

	Convey("When you load a map into a struct", t, func() {
		LoadStruct(&a, map[string]interface{}{"MyString": "abc", "MyInt": 234, "my_json": "xyz", "field": "missing"})
		Convey("Then the values load accurately", func() {
			So(a.MyString, ShouldEqual, "abc")
			So(a.MyInt, ShouldEqual, 234)
			So(a.MyJson, ShouldEqual, "xyz")
		})
	})
}

func TestLoadStructByBytes(t *testing.T) {

	Convey("When you load bytes into a struct", t, func() {
		var a AStruct
		b, err := ToBytes(map[string]interface{}{"MyString": "abc", "MyInt": 234, "my_json": "xyz", "field": "missing"}, false)
		So(err, ShouldBeNil)
		Load(&a, b)
		Convey("Then the values load accurately", func() {
			So(a.MyString, ShouldEqual, "abc")
			So(a.MyInt, ShouldEqual, 234)
			So(a.MyJson, ShouldEqual, "xyz")
		})
	})
}
