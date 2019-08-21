package client

import (
	"context"
	"encoding/json"
	"pg-dg-test/pkg/util"
	"strconv"

	"github.com/dgraph-io/dgo"
	"github.com/dgraph-io/dgo/protos/api"
	"google.golang.org/grpc"
)

const EDGE_IP = "src_ip"
const EDGE_DOMAIN = "src_ip_domain"

type DgraphClient struct {
	db *dgo.Dgraph
	Client
}

// Note:
// Ids are passed as strings to maintain a compatible interface with dgraph
// All errors are fatal

func NewDgClient(conn_str string) *DgraphClient {

	conn, err := grpc.Dial(conn_str, grpc.WithInsecure())
	util.CheckError(err)
	db := dgo.NewDgraphClient(api.NewDgraphClient(conn))
	c := &DgraphClient{
		db: db,
	}
	err = c.SetSchema()
	util.CheckError(err)
	return c
}

// Set/Update schema definition
func (c *DgraphClient) SetSchema() error {

	return c.db.Alter(context.Background(), &api.Operation{
		Schema: `
		name: string @index(exact) .
		addr: string @index(exact) .
		type: int @index(int) .
		domain: string @index(exact) .
		timestamp: int @index(int) .
		src_ip: uid @reverse .
		src_ip_domain: uid @reverse .
		`,
	})
}

// Adds a new IPAddr node
func (c *DgraphClient) AddIp(ip string) string {

	tmp := IPAddr{
		UID:  "_:uid",
		Type: TypeAddress,
		Addr: ip,
	}

	p1, err := json.Marshal(tmp)
	util.CheckError(err)
	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   p1,
	}
	res, err := c.db.NewTxn().Mutate(context.Background(), mu)
	util.CheckError(err)
	return res.Uids["uid"]
}

// Adds a new Domain node
func (c *DgraphClient) AddDomain(fqdn string) string {

	tmp := Domain{
		UID:    "_:uid",
		Type:   TypeDomain,
		Domain: fqdn,
	}

	p1, err := json.Marshal(tmp)
	util.CheckError(err)
	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   p1,
	}
	res, err := c.db.NewTxn().Mutate(context.Background(), mu)
	util.CheckError(err)
	return res.Uids["uid"]
}

// Adds a new Event node
func (c *DgraphClient) AddEvent(ts int64) string {

	tmp := Event{
		UID:       "_:uid",
		Type:      TypeEvent,
		Timestamp: ts,
	}

	p1, err := json.Marshal(tmp)
	util.CheckError(err)
	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   p1,
	}
	res, err := c.db.NewTxn().Mutate(context.Background(), mu)
	util.CheckError(err)
	return res.Uids["uid"]
}

// Retrieve uid for ip address
func (c *DgraphClient) ResolveAddr(ip string) string {

	params := map[string]string{"$ip": ip, "$type": "1"}

	resp, err := c.db.NewTxn().QueryWithVars(
		context.Background(),
		`query addr($ip: string, $type: int) {
  			nodes(func: eq(addr, $ip)) 
				@filter(eq(type, $type)) {
			    	uid
  				}
		}
		`,
		params,
	)
	util.CheckError(err)

	var n AddrCollection
	err = json.Unmarshal(resp.Json, &n)
	util.CheckError(err)
	if len(n.Nodes) > 0 {
		return n.Nodes[0].UID
	}
	return ""
}

// Retrieve uid for domain
func (c *DgraphClient) ResolveDomain(domain string) string {

	params := map[string]string{"$fqdn": domain, "$type": "2"}

	resp, err := c.db.NewTxn().QueryWithVars(
		context.Background(),
		`query addr($fqdn: string, $type: int) {
  			nodes(func: eq(domain, $fqdn)) 
				@filter(eq(type, $type)) {
			    	uid
  				}
		}
		`,
		params,
	)
	util.CheckError(err)

	var n DomainCollection
	err = json.Unmarshal(resp.Json, &n)
	util.CheckError(err)
	if len(n.Nodes) > 0 {
		return n.Nodes[0].UID
	}
	return ""
}

// Retrieve uid for event
func (c *DgraphClient) ResolveEvent(ts int64) string {

	params := map[string]string{"$ts": strconv.FormatInt(ts, 10), "$type": "3"}

	resp, err := c.db.NewTxn().QueryWithVars(
		context.Background(),
		`query addr($ts: int, $type: int) {
  			nodes(func: eq(timestamp, $ts)) 
				@filter(eq(type, $type)) {
			    	uid
  				}
		}
		`,
		params,
	)
	util.CheckError(err)

	var n EventCollection
	err = json.Unmarshal(resp.Json, &n)
	util.CheckError(err)
	if len(n.Nodes) > 0 {
		return n.Nodes[0].UID
	}
	return ""
}

func (c *DgraphClient) LinkAddr(event, ip string) {
	json := `
			{
			"uid": "` + event + `",
				"` + EDGE_IP + `": {
					"uid": "` + ip + `"
			}
		}
		`
	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   []byte(json),
	}
	_, err := c.db.NewTxn().Mutate(context.Background(), mu)
	util.CheckError(err)
}

func (c *DgraphClient) LinkDomain(event, domain string) {
	json := `
			{
			"uid": "` + event + `",
				"` + EDGE_DOMAIN + `": {
					"uid": "` + domain + `"
			}
		}
		`
	mu := &api.Mutation{
		CommitNow: true,
		SetJson:   []byte(json),
	}
	_, err := c.db.NewTxn().Mutate(context.Background(), mu)
	util.CheckError(err)
}
