/* This file is part of VoltDB.
 * Copyright (C) 2008-2018 VoltDB Inc.
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with VoltDB.  If not, see <http://www.gnu.org/licenses/>.
 */

package main

/*
 * Program testing how go handles TLS handshake and server certificate validation.
 * Note: couldn't use main_test.go in same directory because it can't be used under GoLand debugger.
 *
 * Examples:
 *  go run . -h
 *  go run . -pem=/Users/rdykiel/ssl/server/volt.certstore.pem
 *  go run . -altserver=voltdb.com -pem=/Users/rdykiel/ssl/server/volt.certstore.pem
 *  go run . -altserver=foo.voltdb.com -pem=/Users/rdykiel/ssl/server/volt.certstore.pem
 *  go run . -altserver=goo.voltdb.com -pem=/Users/rdykiel/ssl/server/volt.certstore.pem
 *  go run . -insecure
 *
 * Certificate validation: if insecure = false, go expects to find server or altServer in the
 * SubjectAlternativeName extension of the PEM certificate, e.g.:
 *
 * 1: ObjectId: 2.5.29.17 Criticality=false
 * SubjectAlternativeName [
 *   IPAddress: 127.0.0.1
 *   DNSName: voltdb.com
 *   DNSName: foo.voltdb.com
 *   DNSName: bar.voltdb.com
 * ]
 */

import (
	"crypto/tls"
	"database/sql/driver"
	"errors"
	"flag"
	"github.com/VoltDB/voltdb-client-go/voltdbclient"
	"github.com/kr/pretty"
	"log"
)

func tlsConnect(server string, altServer string, pemPath string, insecure bool) {
	log.Printf("Connecting to %s: altServer %s, pemPath %s, insecure %t",
		server, altServer, pemPath, insecure)

	// Keep tlsConfig nil unless altServer is provided: in this case we request to
	// validate the alternate name instead of the server name
	var tlsConfig *tls.Config
	if altServer != "" {
		tlsConfig = &tls.Config{
			InsecureSkipVerify: insecure,
			ServerName:         altServer,
		}
	}

	conn, err := voltdbclient.OpenTLSConn(server, voltdbclient.ClientConfig{
		PEMPath:            pemPath,
		TLSConfig:          tlsConfig,
		InsecureSkipVerify: insecure,
		ConnectTimeout:     60000000,
	})
	if err != nil {
		log.Fatal(err)
	} else if conn == nil {
		log.Fatal(errors.New("no connection"))
	} else {
		log.Printf("connected to: %s", server)
	}

	var params []driver.Value
	for _, s := range []interface{}{"PAUSE_CHECK", int32(0)} {
		params = append(params, s)
	}

	vr, err := conn.Query("@Statistics", params)
	if err != nil {
		log.Fatal(err)
	}
	pretty.Print(vr)
	log.Println()
	log.Printf("SUCCESS")
	//defer conn.Close()
}

func main() {
	server := flag.String("server", "127.0.0.1", "Server name or IP address")
	altServer := flag.String("altserver", "", "Server name to validate instead of server")
	pemPath := flag.String("pem", "", "Full path to PEM file")
	insecure := flag.Bool("insecure", false, "If true, bypass validation")

	flag.Parse()

	tlsConnect(*server, *altServer, *pemPath, *insecure)
}
