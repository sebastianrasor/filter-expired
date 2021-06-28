/*
 * Copyright (c) 2021 Sebastian Riley Rasor <https://www.sebastianrasor.com/contact>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package main

import (
	"bufio"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var version string

var outputChannel chan string

var filters = map[string]func(string, []string){
	"mail-from": filterMailFrom,
	"rcpt-to": filterRcptTo,
}

func produceOutput(msgType string, sessionId string, token string, format string, parameter ...string) {
	var out string

	if version < "0.5" {
		out = msgType + "|" + token + "|" + sessionId
	} else {
		out = msgType + "|" + sessionId + "|" + token
	}
	out += "|" + fmt.Sprintf(format)
	for k := range parameter {
		out += "|" + fmt.Sprintf(parameter[k])
	}

	outputChannel <- out
}

func filterMailFrom(sessionId string, params []string) {
	token := params[0]
	sender := params[1]

	db, err := sql.Open("sqlite3", "/var/vmail/_users.sqlite")
	if err != nil {
		produceOutput("filter-result", sessionId, token, "proceed")
		return
	}
	defer db.Close()

	var expire time.Time
	err = db.QueryRow("select expire from users where user||'@'||domain='" + sender + "'").Scan(&expire)
	if err != nil {
		produceOutput("filter-result", sessionId, token, "proceed")
		return
	}

	if time.Now().After(expire) {
		produceOutput("filter-result", sessionId, token, "reject", "550 5.7.1 The email account that you tried to use is disabled")
		return
	} else {
		produceOutput("filter-result", sessionId, token, "proceed")
		return
	}
}

func filterRcptTo(sessionId string, params []string) {
	token := params[0]
	recipient := params[1]

	db, err := sql.Open("sqlite3", "/var/vmail/_users.sqlite")
	if err != nil {
		produceOutput("filter-result", sessionId, token, "proceed")
		return
	}
	defer db.Close()

	var expire time.Time
	err = db.QueryRow("select expire from users where user||'@'||domain='" + recipient + "'").Scan(&expire)
	if err != nil {
		produceOutput("filter-result", sessionId, token, "proceed")
		return
	}

	if time.Now().After(expire) {
		produceOutput("filter-result", sessionId, token, "reject", "550 5.2.1 The email account that you tried to reach is disabled")
		return
	} else {
		produceOutput("filter-result", sessionId, token, "proceed")
		return
	}
}

func filterInit() {
	for k := range filters {
		fmt.Printf("register|filter|smtp-in|%s\n", k)
	}
	fmt.Println("register|ready")
}

func trigger(currentSlice map[string]func(string, []string), atoms []string) {
	if handler, ok := currentSlice[atoms[4]]; ok {
		handler(atoms[5], atoms[6:])
	} else {
		log.Fatalf("invalid phase: %s", atoms[4])
	}
}

func skipConfig(scanner *bufio.Scanner) {
	for {
		if !scanner.Scan() {
			os.Exit(0)
		}
		line := scanner.Text()
		if line == "config|ready" {
			return
		}
	}
}

func main() {
	flag.Parse()
	scanner := bufio.NewScanner(os.Stdin)
	skipConfig(scanner)
	filterInit()

	outputChannel = make(chan string)
	go func() {
		for line := range outputChannel {
			fmt.Println(line)
		}
	}()

	for {
		if !scanner.Scan() {
			os.Exit(0)
		}

		line := scanner.Text()
		atoms := strings.Split(line, "|")
		if len(atoms) < 6 {
			log.Fatalf("missing atoms: %s", line)
		}

		version = atoms[1]

		if atoms[0] != "filter" {
			log.Fatalf("invalid stream: %s", atoms[0])
		}

		trigger(filters, atoms)
	}
}
