package my

import "fmt"

type field struct {
	Field   string  `gorm:"column:Field"`
	Type    string  `gorm:"column:Type"`
	Null    string  `gorm:"column:Null"`
	Key     string  `gorm:"column:Key"`
	Default *string `gorm:"column:Default"`
	Extra   string  `gorm:"column:Extra"`
}

func (f *field) info() string {
	key := fmt.Sprintf("%s %s", f.Field, f.Type)
	if f.Null == "NO" {
		key += " NOT NULL"
	}
	if f.Default != nil {
		key += " DEFAULT " + *(f.Default)
	}
	key += " " + f.Extra
	return key
}
