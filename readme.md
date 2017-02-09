克隆自 https://github.com/derekgr/hivething
更改了TCLIService, 在inf/目录下，并修改了相应的调用。支持Hive2.0+版本
## usage
	import (
	 	hivething "github.com/dazheng/gohive"
	)
	
	func ListTablesAsync() []string {
	  db, err := hivething.Connect("127.0.0.1:10000", hivething.DefaultOptions)
	  if err != nil {
	    // handle
	  }
	  defer db.Close()
	
	  results, err := db.Query("SHOW TABLES")
	  if err != nil {
	      // handle
	  }
	
	  status, err := results.Wait()
	  if err != nil {
	      // handle
	  }
	
	  if status.IsSuccess() {
	      var tableName string
	      for results.Next() {
	          results.Scan(&tableName)
	          append(tables, tableName)
	      }
	  }
	  else {
	      // handle status.Error
	  }
	
	  return tables
	}