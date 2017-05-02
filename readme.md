## gohive
	克隆自 https://github.com/derekgr/hivething
    更改了TCLIService, 在inf/目录下，并修改了相应的调用。支持Hive2.0+版本
## Usage
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