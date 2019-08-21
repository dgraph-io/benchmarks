package client

import (
	"database/sql"
	"pg-dg-test/pkg/util"
	"strconv"
)

type PgClient struct {
	db *sql.DB
	Client
}

// Note:
// Ids are passed as strings to maintain a compatible interface with dgraph
// All errors are fatal

func NewPgClient(conn_str string) *PgClient {
	db, err := sql.Open("postgres", conn_str)
	util.CheckError(err)

	return &PgClient{
		db: db,
	}
}

// close connection
func (c *PgClient) Close() {
	c.db.Close()
}

// Adds a new IPAddr node
func (c *PgClient) AddIp(ip string) string {

	var lastInsertId int64
	err := c.db.QueryRow("INSERT INTO addr(addr) VALUES($1) returning id_addr;", ip).Scan(&lastInsertId)
	util.CheckError(err)
	return strconv.FormatInt(lastInsertId, 10)
}

// Adds a new Domain node
func (c *PgClient) AddDomain(fqdn string) string {

	var lastInsertId int64
	err := c.db.QueryRow("INSERT INTO domain(domain) VALUES($1) returning id_domain;", fqdn).Scan(&lastInsertId)
	util.CheckError(err)
	return strconv.FormatInt(lastInsertId, 10)
}

// Adds a new Event node
func (c *PgClient) AddEvent(ts int64) string {

	var lastInsertId int64
	err := c.db.QueryRow("INSERT INTO event(ts) VALUES($1) returning id_event;", ts).Scan(&lastInsertId)
	util.CheckError(err)
	return strconv.FormatInt(lastInsertId, 10)
}

// Retrieve uid for ip address
func (c *PgClient) ResolveAddr(ip string) string {

	var id int64
	err := c.db.QueryRow("select id_addr from addr where addr=$1", ip).Scan(&id)
	util.CheckError(err)
	return strconv.FormatInt(id, 10)
}

// Retrieve uid for domain
func (c *PgClient) ResolveDomain(domain string) string {

	var id int64
	err := c.db.QueryRow("select id_domain from domain where domain=$1", domain).Scan(&id)
	util.CheckError(err)
	return strconv.FormatInt(id, 10)
}

// Links an event entry with the ip
func (c *PgClient) LinkAddr(event, ip string) {
	_, err := c.db.Exec("update event set fk_src_addr=$1 where id_event=$2", ip, event)
	util.CheckError(err)
}

// Links an event entry with the domain
func (c *PgClient) LinkDomain(event, domain string) {
	_, err := c.db.Exec("update event set fk_src_domain=$1 where id_event=$2", domain, event)
	util.CheckError(err)
}
