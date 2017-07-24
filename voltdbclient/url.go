package voltdbclient

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

var errBadURL = errors.New("voltdb: bad connection url")

func parseURL(src string) (*url.URL, error) {
	u, err := url.Parse(src)
	if err != nil {
		return nil, err
	}
	if u.User == nil {
		u.User = &url.Userinfo{}
	}
	if u.Scheme != "voltdb" {
		if u.Host == "" {
			if u.Opaque != "" {
				u.Host = fmt.Sprintf("%s:%s", u.Scheme, u.Opaque)
			} else {
				u.Host = fmt.Sprintf("%s:%s", u.Scheme, "21212")
			}
		}
	} else {
		if getPort(u.Host) == "" {
			u.Host = u.Host + ":21212"
		}
	}
	return u, nil
}

// getPort finds the port number of url host.
// this method is same with the code block of the url.Port() method. you can
// find this metod inside the Port method of the url package.
// this method is used inside the client package to support go versions <1.8
func getPort(host string) string{
	index := strings.IndexByte(host, ':')
	if index == -1 {
		return ""
	}
	if i := strings.Index(host, "]:"); i != -1 {
		return host[i+len("]:"):]
	}
	if strings.Contains(host, "]") {
		return ""
	}
	return host[index+len(":"):]
}
