package voltdbclient

import (
	"testing"
)

func TestParseURL(t *testing.T) {
	sample := []struct {
		conn                       string
		pass                       bool
		scheme, user, passwd, host string
	}{
		{"https://localhost:21212", true, "https", "", "", "localhost:21212"},
		{"http://localhost:21212", true, "http", "", "", "localhost:21212"},
		{"localhost:21212", true, "localhost", "", "", "localhost:21212"},
		{"127.0.0.1:21212", false, "voltdb", "", "", "localhost:21212"},
		{"voltdb://", true, "voltdb", "", "", ":21212"},
		{"voltdb://localhost", true, "voltdb", "", "", "localhost:21212"},
		{"voltdb://localhost:21212", true, "voltdb", "", "", "localhost:21212"},
		{"voltdb://localhost/mydb", true, "voltdb", "", "", "localhost:21212"},
		{"voltdb://user@localhost", true, "voltdb", "user", "", "localhost:21212"},
		{"voltdb://user:secret@localhost", true, "voltdb", "user", "secret", "localhost:21212"},
	}

	for _, s := range sample {
		u, err := parseURL(s.conn)
		if s.pass {
			if err != nil {
				t.Fatal(err)
			}
			if u.Scheme != s.scheme {
				t.Errorf("expected %s got %s", s.scheme, u.Scheme)
			}
			if u.User.Username() != s.user {
				t.Errorf("expected %s got %s", s.user, u.User)
			}
			p, _ := u.User.Password()
			if p != s.passwd {
				t.Errorf("expected %s got %s", s.passwd, p)
			}
			if u.Host != s.host {
				t.Errorf("expected %s got %s", s.host, u.Host)
			}
		} else {
			if err == nil {
				t.Fatal("expected an error")
			}
		}
	}
}
