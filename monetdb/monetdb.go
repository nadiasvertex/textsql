package monetdb

// #include <malloc.h>
// #include "../vendor/monetdblite/embedded.h"
// #cgo linux LDFLAGS: -L../vendor/monetdblite/ubuntu-17.10 -lmonetdb5 -lm -ldl
import "C"
import (
	"errors"
	"unsafe"
)

type Connection struct {
	Handle C.monetdb_connection
}

type ResultSet struct {
	Results *C.monetdb_result
}

func Startup() error {
	r := C.monetdb_startup(nil, 0, 0)
	if r != nil {
		return errors.New(C.GoString(r))
	}

	return nil
}

func Connect() Connection {
	return Connection{Handle: C.monetdb_connect()}
}

func (conn *Connection) Close() {
	C.monetdb_disconnect(conn.Handle)
}

func (conn *Connection) Execute(sql string) error {
	c_sql := C.CString(sql)
	defer C.free(unsafe.Pointer(c_sql))

	var result *C.monetdb_result
	r := C.monetdb_query(conn.Handle, c_sql, 1, &result, nil, nil)
	if r != nil {
		return errors.New(C.GoString(r))
	}

	if result != nil {
		C.monetdb_cleanup_result(conn.Handle, result)
	}

	return nil

}
