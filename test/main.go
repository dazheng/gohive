package main

import (
	"fmt"

	"github.com/dazheng/gohive"
)

func main() {
	conn, err := gohive.Connect("127.0.0.1:10000", gohive.DefaultOptions)
	if err != nil {
		fmt.Errorf("Connect error %v", err)
	}
	defer conn.Close()

	_, err = conn.Exec("create table if not exists t(c1 int)")
	if err != nil {
		fmt.Errorf("Connection.Exec error: %v", err)
	}
}
