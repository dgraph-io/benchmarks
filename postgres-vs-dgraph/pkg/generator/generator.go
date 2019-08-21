package generator

import (
	"encoding/csv"
	"math/rand"
	"net"
	"os"
	"pg-dg-test/pkg/util"
	"time"
)

var topTLD = []string{"com", "net", "org", "eu", "pt", "es", "us", "vu", "cn", "jp", "de", "it", "ie", "br", "au", "in", "info"}
var tldMap = "abcdefghijklmnopqrstuvwxyz"

// Generate list of random Ipv4 addresses
func GenerateIpv4(count uint32) []string {
	tmp := make(map[uint32]string, count)
	for count > 0 {
		rand.Seed(time.Now().UnixNano() + rand.Int63())
		i := rand.Uint32()
		if _, e := tmp[i]; !e {
			ip := net.IPv4(byte(i>>24), byte(i>>16), byte(i>>8), byte(i&0xF))
			tmp[i] = ip.String()
			count--
		}
	}

	result := make([]string, 0)
	for _, v := range tmp {
		result = append(result, v)

	}
	return result
}

// Generate a list of random domain names
func GenerateFQDN(minlen, maxlen, count int) []string {
	result := make([]string, 0)
	for count > 0 {
		result = append(result, randomString(minlen, maxlen)+"."+getTld())
		count--
	}
	return result
}

// Save list to file
func Save(list []string, fn string) error {
	f, err := os.Create(fn)
	util.CheckError(err)
	defer f.Close()

	writer := csv.NewWriter(f)

	for _, v := range list {
		err := writer.Write([]string{v})
		util.CheckError(err)
	}
	return nil
}

// PickRandom picks a random item from a list.
func PickRandom(list []string) string {
	l := rand.Intn(len(list))
	return list[l]
}

func randomString(minlen, maxlen int) string {
	rand.Seed(time.Now().UnixNano())
	l := rand.Intn(maxlen-minlen+1) + minlen
	result := ""
	for i := 0; i <= l; i++ {
		k := rand.Intn(len(tldMap))
		result = result + string(tldMap[k])
	}
	return result
}

func getTld() string {
	return topTLD[rand.Intn(len(topTLD))]
}
