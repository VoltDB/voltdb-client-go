package voltdbclient

import (
	"testing"
	"bytes"
	"io/ioutil"
)

func TestLoginRequest(t *testing.T) {
	config := ClientConfig{"hello", "world"}
	var loginBytes []byte
	loginBuf := bytes.NewBuffer(loginBytes)
	client := Client{&config, nil, loginBuf, nil, nil, 0}
	login, err := serializeLoginMessage(client.config.username, client.config.password)
	check(t, err)
	client.writeLoginMessage(&login)

	fileBytes, err := ioutil.ReadFile("./test_resources/authentication_request_sha256.msg")
	check(t, err)

	if !bytes.Equal(loginBuf.Bytes(), fileBytes) {
		t.Fatal("login message doesn't match expected contents")
	}
}

func TestLoginResponse(t *testing.T) {
	b, err := ioutil.ReadFile("./test_resources/authentication_response.msg")
	check(t, err)
	reader := bytes.NewReader(b)
	config := ClientConfig{"", ""}
	client := Client{&config, reader, nil, nil, nil, 0}
	connData, err := client.readLoginResponse()
	check(t, err)
	// consider as passed if returns non nil connection data
	if (connData == nil) {
		t.Fatal("login response didn't return connection data")
	}
}

func check(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err.Error())
	}
}
