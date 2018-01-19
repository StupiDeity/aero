package str

import (
	"strings"
	"unicode"

	"github.com/azer/snakecase"
)

func SnakeCase(text string) string {
	return snakecase.SnakeCase(text)
}

func UrlCase(text string) string {
	var words []string
	l := 0
	for s := text; s != ""; s = s[l:] {
		l = strings.IndexFunc(s[1:], unicode.IsUpper) + 1
		if l <= 0 {
			l = len(s)
		}
		words = append(words, s[:l])
	}

	url := strings.ToLower(strings.Join(words, "-"))
	url = strings.Replace(url, "--", "-", -1) // pure hack. todo: reg-ex
	return url
}

func SentenceCase(text string) string {
	if len(text) > 0 {
		u := []rune(text)
		u[0] = unicode.ToUpper(u[0])
		return string(u)
	}

	return text
}
