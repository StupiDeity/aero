package csv

import (
	"encoding/csv"
	"io"
	"regexp"
	"strconv"
	"strings"
)

type column struct {
	name     string
	key      string
	isInt    bool
	isFloat  bool
	isBool   bool
	isString bool
}

func (j *column) findType(val string) {

	// don't try to find the type for missing values
	if val == "" {
		return
	}

	// is it int?
	_, err := strconv.Atoi(val)
	if err == nil {
		j.isInt = true
		return
	}

	// is it float?
	_, err = strconv.ParseFloat(val, 64)
	if err == nil {
		j.isFloat = true
		return
	}

	// is it bool?
	_, err = strconv.ParseBool(val)
	if err == nil {
		j.isBool = true
		return
	}

	// if none of above then it must be string
	j.isString = true
}

func (j *column) bestType() string {
	switch {
	case j.isInt && !j.isFloat && !j.isBool && !j.isString:
		return "int"
	case !j.isInt && j.isFloat && !j.isBool && !j.isString:
		return "float"
	case !j.isInt && !j.isFloat && j.isBool && !j.isString:
		return "bool"
	}
	return "string"
}

var KeyFunction func(string) string

func init() {

	// Default implementation of KeyFunction
	reg, _ := regexp.Compile("\\s+")
	regAscii, _ := regexp.Compile("[^A-Za-z0-9_/.]")
	regMult, _ := regexp.Compile("_+")

	KeyFunction = func(inp string) string {
		inp = strings.TrimSpace(inp)
		inp = strings.ToLower(inp)
		inp = reg.ReplaceAllString(inp, "_")     //  whitepsaces => _
		inp = regMult.ReplaceAllString(inp, "_") //  multiple _ => single _
		inp = regAscii.ReplaceAllString(inp, "") // non-alphanumeric => empty

		return inp
	}
}

func ToJson(r io.Reader) ([]string, []map[string]interface{}, error) {

	csvReader := csv.NewReader(r)
	csvReader.TrimLeadingSpace = true

	// names = expect in the first row
	names, err := csvReader.Read()
	if err != nil {
		return nil, nil, err
	}

	// convert names to keys (for json using the KeyFunction fn)
	cols := make([]column, len(names))
	for i, n := range names {
		cols[i].name = n
		cols[i].key = KeyFunction(n)
	}

	// first, load everything as string
	// but also try to find the underlying best type match
	rows := make([]map[string]interface{}, 0)

	for {
		data, err := csvReader.Read()
		if err == io.EOF {
			break
		}

		j := make(map[string]interface{})
		for i, d := range data {
			j[cols[i].key] = d  // store string value
			cols[i].findType(d) // try to decipher the type
		}
		rows = append(rows, j)
	}

	// loop through everything again, and try to convert
	// each string item to a type that is in accordance to
	// what was found earlier
	var j map[string]interface{}
	for i := 0; i < len(rows); i++ {
		j = rows[i]
		for k := 0; k < len(cols); k++ {
			key := cols[k].key
			item, ok := j[key]
			if !ok {
				break
			}
			str := item.(string)
			switch cols[k].bestType() {
			case "int":
				if len(str) != 0 {
					j[key], _ = strconv.Atoi(str)
				} else {
					j[key] = nil // treat empty values as null
				}
			case "float":
				if len(str) != 0 {
					j[key], _ = strconv.ParseFloat(str, 64)
				} else {
					j[key] = nil // treat empty values as null
				}
			case "bool":
				if len(str) != 0 {
					j[key], _ = strconv.ParseBool(str)
				} else {
					j[key] = nil // treat empty values as null
				}
			}
		}
	}

	// col names
	c := make([]string, len(cols))
	for i, col := range cols {
		c[i] = col.key
	}

	return c, rows, nil
}
