package cstr

import (
	"fmt"
	"net/url"
)

type Connecter interface {
	Cstr() string
}

type Couchbase struct {
	Host   string
	Port   int
	Bucket string
}

func (c Couchbase) Cstr() string {
	return fmt.Sprintf("couchbase://%s:%d", c.Host, c.Port)
}

type Redis struct {
	Host string
	Port int
	Db   int
	Name string `conf:"optional"`
}

func (r Redis) Cstr() string {
	return ""
}

type Memcache struct {
	Host string
	Port int
}

func (m Memcache) Cstr() string {
	return fmt.Sprintf("%s:%d",
		m.Host, m.Port,
	)
}

type Sqlite struct {
	Path string
}

func (s Sqlite) Cstr() string {
	return s.Path
}

type Mysql struct {
	Host     string
	Port     int
	Db       string
	Username string
	Password string
	Timezone string `conf:"optional"`
}

func (m Mysql) Cstr() string {
	return fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true&loc=%s",
		m.Username, m.Password,
		m.Host, m.Port, m.Db,
		url.QueryEscape(m.Timezone),
	)
}

type Mongodb struct {
	Host     string
	Port     int
	Db       string
	Username string `conf:"optional"`
	Password string `conf:"optional"`
	Replicas string `conf:"optional"`
	Options  string `conf:"optional"`
}

func (m Mongodb) Cstr() string {

	replicas := m.Replicas
	if replicas != "" {
		replicas = "," + replicas
	}

	options := m.Options
	if options != "" {
		options = "?" + options
	}

	auth := ""
	if m.Username != "" || m.Password != "" {
		auth = m.Username + ":" + m.Password + "@"
	}

	return fmt.Sprintf("mongodb://%s%s:%d%s/%s%s",
		auth, m.Host, m.Port, replicas,
		m.Db, options,
	)
}
