// misc data-structure utility functions
package ds

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fatih/structs"
	"github.com/mitchellh/mapstructure"
	"github.com/pquerna/ffjson/ffjson"
)

func init() {
	structs.DefaultTagName = "json"
}

func Load(addr interface{}, b []byte) error {
	return json.Unmarshal(b, addr)
}

func Load2(addr interface{}, b []byte, enc string) error {
	switch enc {
	case "json":
		return json.Unmarshal(b, addr)
	case "ffjson":
		return ffjson.Unmarshal(b, addr)
	default:
		panic(fmt.Sprintf("Unknown decoding %s", enc))
		return errors.New("Unknown decoding " + enc)
	}
}

func LoadStruct(addr interface{}, m map[string]interface{}) error {
	// TODO: use same decoder for all objects of same type?
	decoder, err := mapstructure.NewDecoder(&mapstructure.DecoderConfig{
		TagName:          "json",
		Result:           addr,
		WeaklyTypedInput: false})

	if err != nil {
		return err
	}

	return decoder.Decode(m)
}

// Encode to bytes

func ToBytes(o interface{}, jsonPretty bool) ([]byte, error) {
	if !jsonPretty {
		return json.Marshal(o)
	} else {
		return json.MarshalIndent(o, "", "\t")
	}
}

func ToBytes2(o interface{}, enc string) ([]byte, error) {
	switch enc {
	case "json":
		return json.Marshal(o)
	case "ffjson":
		return ffjson.Marshal(o)
	default:
		panic(fmt.Sprintf("Unknown encoding %s", enc))
		return nil, errors.New("Unknown encoding " + enc)
	}
}

// TODO: this method is very unoptimized
// It converts to json first, and then loads a map
// Need to do this directly using reflection
func StructToMap(addr interface{}) map[string]interface{} {
	b, err := ToBytes(addr, false)
	if err != nil {
		panic("json marshall error")
	}

	var mp map[string]interface{}
	err = Load(&mp, b)
	if err != nil {
		panic("map conversion error")
	}

	return mp
}
