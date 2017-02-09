package main

import (
	"fmt"

	"github.com/dazheng/gohive"
)

func main() {

	conn, err := gohive.Connect("127.0.0.1:10000", gohive.DefaultOptions)
	if err != nil {
		fmt.Printf("Connect error %v", err)
	}

	rows, err := conn.Query("SHOW TABLES")
	if err != nil {
		fmt.Printf("Connection.Query error: %v", err)
	}

	status, err := rows.Wait()
	if err != nil {
		fmt.Printf("Connection.Wait error: %v", err)
	}

	if !status.IsSuccess() {
		fmt.Printf("Unsuccessful query execution: %v", status)
	}
}
