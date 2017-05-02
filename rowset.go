package gohive

import (
	"errors"
	"fmt"
	"log"
	"time"

	inf "github.com/dazheng/gohive/inf"

	"git.apache.org/thrift.git/lib/go/thrift"
)

type rowSet struct {
	thrift    *inf.TCLIServiceClient
	operation *inf.TOperationHandle
	options   Options

	columns    []*inf.TColumnDesc
	columnStrs []string

	offset  int
	rowSet  *inf.TRowSet
	hasMore bool
	ready   bool

	nextRow []interface{}
}

// A RowSet represents an asyncronous hive operation. You can
// Reattach to a previously submitted hive operation if you
// have a valid thrift client, and the serialized Handle()
// from the prior operation.
type RowSet interface {
	Handle() ([]byte, error)
	Columns() []string
	Next() bool
	Scan(dest ...interface{}) error
	Poll() (*Status, error)
	Wait() (*Status, error)
}

// Represents job status, including success state and time the
// status was updated.
type Status struct {
	state *inf.TOperationState
	Error error
	At    time.Time
}

func newRowSet(thrift *inf.TCLIServiceClient, operation *inf.TOperationHandle, options Options) RowSet {
	return &rowSet{thrift, operation, options, nil, nil, 0, nil, true, false, nil}
}

// Construct a RowSet for a previously submitted operation, using the prior operation's Handle()
// and a valid thrift client to a hive service that is aware of the operation.
func Reattach(conn *Connection, handle []byte) (RowSet, error) {
	operation, err := deserializeOp(handle)
	if err != nil {
		return nil, err
	}

	return newRowSet(conn.thrift, operation, conn.options), nil
}

// Issue a thrift call to check for the job's current status.
func (r *rowSet) Poll() (*Status, error) {
	req := inf.NewTGetOperationStatusReq()
	req.OperationHandle = r.operation

	resp, err := r.thrift.GetOperationStatus(req)
	if err != nil {
		return nil, fmt.Errorf("Error getting status: %+v, %v", resp, err)
	}

	if !isSuccessStatus(resp.Status) {
		return nil, fmt.Errorf("GetStatus call failed: %s", resp.Status.String())
	}

	if resp.OperationState == nil {
		return nil, errors.New("No error from GetStatus, but nil status!")
	}

	return &Status{resp.OperationState, nil, time.Now()}, nil
}

// Wait until the job is complete, one way or another, returning Status and error.
func (r *rowSet) Wait() (*Status, error) {
	for {
		status, err := r.Poll()

		if err != nil {
			return nil, err
		}

		if status.IsComplete() {
			if status.IsSuccess() {
				// Fetch operation metadata.
				metadataReq := inf.NewTGetResultSetMetadataReq()
				metadataReq.OperationHandle = r.operation

				metadataResp, err := r.thrift.GetResultSetMetadata(metadataReq)
				if err != nil {
					return nil, err
				}

				if !isSuccessStatus(metadataResp.Status) {
					return nil, fmt.Errorf("GetResultSetMetadata failed: %s", metadataResp.Status.String())
				}

				r.columns = metadataResp.Schema.Columns
				r.ready = true

				return status, nil
			}
			return nil, fmt.Errorf("Query failed execution: %s", status.state.String())
		}

		time.Sleep(time.Duration(r.options.PollIntervalSeconds) * time.Second)
	}
}

func (r *rowSet) waitForSuccess() error {
	if !r.ready {
		status, err := r.Wait()
		if err != nil {
			return err
		}
		if !status.IsSuccess() || !r.ready {
			return fmt.Errorf("Unsuccessful query execution: %+v", status)
		}
	}

	return nil
}

// Prepares a row for scanning into memory, by reading data from hive if
// the operation is successful, blocking until the operation is
// complete, if necessary.
// Returns true is a row is available to Scan(), and false if the
// results are empty or any other error occurs.
func (r *rowSet) Next() bool {
	if err := r.waitForSuccess(); err != nil {
		return false
	}

	if r.rowSet == nil || r.offset >= len(r.rowSet.Rows) {
		if !r.hasMore {
			return false
		}

		fetchReq := inf.NewTFetchResultsReq()
		fetchReq.OperationHandle = r.operation
		fetchReq.Orientation = inf.TFetchOrientation_FETCH_NEXT
		fetchReq.MaxRows = r.options.BatchSize

		resp, err := r.thrift.FetchResults(fetchReq)
		if err != nil {
			log.Printf("FetchResults failed: %v\n", err)
			return false
		}

		if !isSuccessStatus(resp.Status) {
			log.Printf("FetchResults failed: %s\n", resp.Status.String())
			return false
		}

		r.offset = 0
		r.rowSet = resp.Results
		r.hasMore = *resp.HasMoreRows
	}
	fmt.Println(r.rowSet.String(), r.offset)
	row := r.rowSet.Rows[r.offset]
	r.nextRow = make([]interface{}, len(r.Columns()))

	if err := convertRow(row, r.nextRow); err != nil {
		log.Printf("Error converting row: %v", err)
		return false
	}
	r.offset++

	return true
}

// Scan the last row prepared via Next() into the destination(s) provided,
// which must be pointers to value types, as in database.sql. Further,
// only pointers of the following types are supported:
// 	- int, int16, int32, int64
// 	- string, []byte
// 	- float64
//	 - bool
func (r *rowSet) Scan(dest ...interface{}) error {
	// TODO: Add type checking and conversion between compatible
	// types where possible, as well as some common error checking,
	// like passing nil. database/sql's method is very convenient,
	// for example: http://golang.org/src/pkg/database/sql/convert.go, like 85
	if r.nextRow == nil {
		return errors.New("No row to scan! Did you call Next() first?")
	}

	if len(dest) != len(r.nextRow) {
		return fmt.Errorf("Can't scan into %d arguments with input of length %d", len(dest), len(r.nextRow))
	}

	for i, val := range r.nextRow {
		d := dest[i]
		switch dt := d.(type) {
		case *string:
			switch st := val.(type) {
			case string:
				*dt = st
			default:
				*dt = fmt.Sprintf("%v", val)
			}
		case *[]byte:
			*dt = []byte(val.(string))
		case *int:
			*dt = int(val.(int32))
		case *int64:
			*dt = val.(int64)
		case *int32:
			*dt = val.(int32)
		case *int16:
			*dt = val.(int16)
		case *float64:
			*dt = val.(float64)
		case *bool:
			*dt = val.(bool)
		default:
			return fmt.Errorf("Can't scan value of type %T with value %v", dt, val)
		}
	}

	return nil
}

// Returns the names of the columns for the given operation,
// blocking if necessary until the information is available.
func (r *rowSet) Columns() []string {
	if r.columnStrs == nil {
		if err := r.waitForSuccess(); err != nil {
			return nil
		}

		ret := make([]string, len(r.columns))
		for i, col := range r.columns {
			ret[i] = col.ColumnName
		}

		r.columnStrs = ret
	}

	return r.columnStrs
}

// Return a serialized representation of an identifier that can later
// be used to reattach to a running operation. This identifier and
// serialized representation should be considered opaque by users.
func (r *rowSet) Handle() ([]byte, error) {
	return serializeOp(r.operation)
}

func convertRow(row *inf.TRow, dest []interface{}) error {
	if len(row.ColVals) != len(dest) {
		return fmt.Errorf("Returned row has %d values, but scan row has %d", len(row.ColVals), len(dest))
	}

	for i, col := range row.ColVals {
		val, err := convertColumn(col)
		if err != nil {
			return fmt.Errorf("Error converting column %d: %v", i, err)
		}
		dest[i] = val
	}

	return nil
}

func convertColumn(col *inf.TColumnValue) (interface{}, error) {
	switch {
	case col.StringVal.IsSetValue():
		return col.StringVal.GetValue(), nil
	case col.BoolVal.IsSetValue():
		return col.BoolVal.GetValue(), nil
	case col.ByteVal.IsSetValue():
		return int64(col.ByteVal.GetValue()), nil
	case col.I16Val.IsSetValue():
		return int32(col.I16Val.GetValue()), nil
	case col.I32Val.IsSetValue():
		return col.I32Val.GetValue(), nil
	case col.I64Val.IsSetValue():
		return col.I64Val.GetValue(), nil
	case col.DoubleVal.IsSetValue():
		return col.DoubleVal.GetValue(), nil
	default:
		return nil, fmt.Errorf("Can't convert column value %v", col)
	}
}

// Returns a string representation of operation status.
func (s Status) String() string {
	if s.state == nil {
		return "unknown"
	}
	return s.state.String()
}

// Returns true if the job has completed or failed.
func (s Status) IsComplete() bool {
	if s.state == nil {
		return false
	}

	switch *s.state {
	case inf.TOperationState_FINISHED_STATE,
		inf.TOperationState_CANCELED_STATE,
		inf.TOperationState_CLOSED_STATE,
		inf.TOperationState_ERROR_STATE:
		return true
	}

	return false
}

// Returns true if the job compelted successfully.
func (s Status) IsSuccess() bool {
	if s.state == nil {
		return false
	}

	return *s.state == inf.TOperationState_FINISHED_STATE
}

func deserializeOp(handle []byte) (*inf.TOperationHandle, error) {
	ser := thrift.NewTDeserializer()
	var val inf.TOperationHandle

	if err := ser.Read(&val, handle); err != nil {
		return nil, err
	}

	return &val, nil
}

func serializeOp(operation *inf.TOperationHandle) ([]byte, error) {
	ser := thrift.NewTSerializer()
	return ser.Write(operation)
}
