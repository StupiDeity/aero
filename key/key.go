package key

import (
	"strings"
)

type KeyFormatter interface {
	Format(key string) string
}

// do not mess with current key
type AsIsFormat struct {
}

func (k AsIsFormat) Format(key string) string {
	return key
}

// remove spaces from this key
type NoSpacesFormat struct {
}

func (k NoSpacesFormat) Format(key string) string {
	return strings.Replace(key, " ", "-", -1)
}
