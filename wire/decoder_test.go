package wire

import (
	"bytes"
	"io/ioutil"
	"testing"
)

func TestDecodeLoginInfo(t *testing.T) {
	b, err := ioutil.ReadFile("fixture/authentication_response.msg")
	if err != nil {
		t.Fatal(err)
	}

	info, err := NewDecoder(bytes.NewReader(b)).Login()
	if err != nil {
		t.Fatal(err)
	}
	if info.HostID != 0 {
		t.Errorf("expected 0 got %d", info.HostID)
	}
	if info.Connection != 2 {
		t.Errorf("expected 2 got %d", info.Connection)
	}
	ip := "127.0.0.1"
	if info.LeaderAddr.IP.String() != ip {
		t.Errorf("expected %s got %s", ip, info.LeaderAddr.IP.String())
	}
	build := "volt_6.1_test_build_string"
	if info.Build != build {
		t.Errorf("expected build got %s", info.Build)
	}
}
