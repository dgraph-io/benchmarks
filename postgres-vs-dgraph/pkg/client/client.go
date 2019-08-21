package client

import (
	"fmt"
	"pg-dg-test/pkg/generator"
)

type Client interface {
	AddIp(ip string) string
	AddDomain(domain string) string
	AddEvent(ts int64) string
	ResolveAddr(ip string) string
	ResolveDomain(domain string) string
	LinkAddr(event, ip string)
	LinkDomain(event, domain string)
}

// Ticker generates N events for the given timestamp.
// It will add addresses or ips if necessary.
func Ticker(c Client, ts int64, runs int, ips, domains []string) {
	for ; runs > 0; runs-- {
		ip := generator.PickRandom(ips)
		domain := generator.PickRandom(domains)

		// read or create ip
		ipID := c.ResolveAddr(ip)
		if len(ipID) == 0 {
			ipID = c.AddIp(ip)
		}

		// read or create domain
		domainID := c.ResolveDomain(domain)
		if len(domainID) == 0 {
			domainID = c.AddDomain(ip)
		}

		eventID := c.AddEvent(ts)

		c.LinkAddr(eventID, ipID)
		c.LinkDomain(eventID, domainID)

		fmt.Printf("Added event %s with ip %s, domain %s\n", eventID, ip, domain)
	}
}
