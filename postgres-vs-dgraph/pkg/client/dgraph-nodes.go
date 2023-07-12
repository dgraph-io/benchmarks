package client

// IPAddr is an IP address.
type IPAddr struct {
	UID  string `json:"uid,omitempty"`
	Type Type   `json:"type"`
	Addr string `json:"addr,omitempty"`
}

// Type is an IP type enum.
type Type int

const (
	// TypeAddress is for addresses.
	TypeAddress = 1
	// TypeDomain is for domains.
	TypeDomain = 2
	// TypeEvent is for events.
	TypeEvent = 3
)

// Domain contains the information for a domain.
type Domain struct {
	UID    string `json:"uid,omitempty"`
	Type   Type   `json:"type"`
	Domain string `json:"domain,omitempty"`
}

// Event contains the information for a domain.
type Event struct {
	UID       string `json:"uid,omitempty"`
	Type      Type   `json:"type"`
	Timestamp int64  `json:"timestamp"`
}

// Timestamp is a time stamp.
type Timestamp struct {
	UID       string `json:"uid,omitempty"`
	Type      Type   `json:"type"`
	Timestamp int64  `json:"timestamp"`
}

// AddrCollection is a collection of addresses.
type AddrCollection struct {
	Nodes []IPAddr `json:"nodes"`
}

// DomainCollection is a collection of domains.
type DomainCollection struct {
	Nodes []Domain `json:"nodes"`
}

// EventCollection is a collection of events.
type EventCollection struct {
	Nodes []Event `json:"nodes"`
}
