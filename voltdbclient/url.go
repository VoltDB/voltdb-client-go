package voltdbclient

import (
	"errors"
	"fmt"
	"net/url"
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
		if u.Port() == "" {
			u.Host = u.Host + ":21212"
		}
	}
	return u, nil
}
