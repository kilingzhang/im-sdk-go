//go:build darwin

package imessage

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"syscall"
	"unsafe"

	"github.com/ebitengine/purego"
)

const (
	sqliteOK         = 0
	sqliteAuth       = 23
	sqliteRow        = 100
	sqliteDone       = 101
	sqliteNull       = 5
	sqliteOpenRO     = 0x00000001
	sqliteOpenRW     = 0x00000002
	sqliteOpenCreate = 0x00000004
	sqliteOpenNoMtx  = 0x00008000

	sqliteTransient = ^uintptr(0)
)

type sqliteLib struct {
	openV2          func(filename string, ppDb *uintptr, flags int32, zVfs uintptr) int32
	close           func(db uintptr) int32
	busyTimeout     func(db uintptr, ms int32) int32
	errcode         func(db uintptr) int32
	errmsg          func(db uintptr) string
	exec            func(db uintptr, sql string, callback uintptr, arg uintptr, errmsg *uintptr) int32
	prepareV2       func(db uintptr, zSql string, nByte int32, ppStmt *uintptr, pzTail uintptr) int32
	bindInt64       func(stmt uintptr, idx int32, val int64) int32
	bindText        func(stmt uintptr, idx int32, val string, nByte int32, destructor uintptr) int32
	bindBlob        func(stmt uintptr, idx int32, val []byte, nByte int32, destructor uintptr) int32
	bindNull        func(stmt uintptr, idx int32) int32
	step            func(stmt uintptr) int32
	columnType      func(stmt uintptr, iCol int32) int32
	columnInt64     func(stmt uintptr, iCol int32) int64
	columnText      func(stmt uintptr, iCol int32) *byte
	columnBlob      func(stmt uintptr, iCol int32) *byte
	columnBytes     func(stmt uintptr, iCol int32) int32
	finalize        func(stmt uintptr) int32
	lastInsertRowID func(db uintptr) int64
}

var loadSQLite = sync.OnceValues(func() (*sqliteLib, error) {
	h, err := purego.Dlopen("/usr/lib/libsqlite3.dylib", purego.RTLD_NOW)
	if err != nil {
		return nil, fmt.Errorf("imessage: dlopen libsqlite3.dylib: %w", err)
	}
	lib := &sqliteLib{}
	purego.RegisterLibFunc(&lib.openV2, h, "sqlite3_open_v2")
	purego.RegisterLibFunc(&lib.close, h, "sqlite3_close")
	purego.RegisterLibFunc(&lib.busyTimeout, h, "sqlite3_busy_timeout")
	purego.RegisterLibFunc(&lib.errcode, h, "sqlite3_errcode")
	purego.RegisterLibFunc(&lib.errmsg, h, "sqlite3_errmsg")
	purego.RegisterLibFunc(&lib.exec, h, "sqlite3_exec")
	purego.RegisterLibFunc(&lib.prepareV2, h, "sqlite3_prepare_v2")
	purego.RegisterLibFunc(&lib.bindInt64, h, "sqlite3_bind_int64")
	purego.RegisterLibFunc(&lib.bindText, h, "sqlite3_bind_text")
	purego.RegisterLibFunc(&lib.bindBlob, h, "sqlite3_bind_blob")
	purego.RegisterLibFunc(&lib.bindNull, h, "sqlite3_bind_null")
	purego.RegisterLibFunc(&lib.step, h, "sqlite3_step")
	purego.RegisterLibFunc(&lib.columnType, h, "sqlite3_column_type")
	purego.RegisterLibFunc(&lib.columnInt64, h, "sqlite3_column_int64")
	purego.RegisterLibFunc(&lib.columnText, h, "sqlite3_column_text")
	purego.RegisterLibFunc(&lib.columnBlob, h, "sqlite3_column_blob")
	purego.RegisterLibFunc(&lib.columnBytes, h, "sqlite3_column_bytes")
	purego.RegisterLibFunc(&lib.finalize, h, "sqlite3_finalize")
	purego.RegisterLibFunc(&lib.lastInsertRowID, h, "sqlite3_last_insert_rowid")
	return lib, nil
})

type permissionError struct {
	err error
}

func (e *permissionError) Error() string { return e.err.Error() }
func (e *permissionError) Unwrap() error { return e.err }

type sqliteError struct {
	code int32
	msg  string
}

func (e *sqliteError) Error() string {
	return fmt.Sprintf("sqlite3 (%d): %s", e.code, e.msg)
}

func isPermissionError(err error) bool {
	var pe *permissionError
	if errors.As(err, &pe) {
		return true
	}
	var se *sqliteError
	if errors.As(err, &se) && se.code == sqliteAuth {
		return true
	}
	return errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES)
}

type sqliteDB struct {
	lib    *sqliteLib
	handle uintptr
}

func openMessagesDB(path string) (*sqliteDB, error) {
	f, err := os.Open(path)
	if err != nil {
		if errors.Is(err, os.ErrPermission) || errors.Is(err, syscall.EPERM) || errors.Is(err, syscall.EACCES) {
			return nil, &permissionError{err: fmt.Errorf("cannot read %s: %w", path, err)}
		}
		return nil, err
	}
	_ = f.Close()

	db, err := openRawSQLite(path, sqliteOpenRO|sqliteOpenNoMtx)
	if err != nil {
		return nil, err
	}
	db.lib.busyTimeout(db.handle, 2000)

	stmt, err := db.prepare("SELECT 1 FROM message LIMIT 1")
	if err != nil {
		_ = db.close()
		return nil, err
	}
	_, err = stmt.step()
	stmt.finalize()
	if err != nil {
		_ = db.close()
		return nil, err
	}
	return db, nil
}

func openReadOnlySQLite(path string) (*sqliteDB, error) {
	db, err := openRawSQLite(path, sqliteOpenRO|sqliteOpenNoMtx)
	if err != nil {
		return nil, err
	}
	db.lib.busyTimeout(db.handle, 2000)
	return db, nil
}

func openReadWriteSQLite(path string) (*sqliteDB, error) {
	db, err := openRawSQLite(path, sqliteOpenRW|sqliteOpenCreate|sqliteOpenNoMtx)
	if err != nil {
		return nil, err
	}
	db.lib.busyTimeout(db.handle, 2000)
	return db, nil
}

func openRawSQLite(path string, flags int32) (*sqliteDB, error) {
	lib, err := loadSQLite()
	if err != nil {
		return nil, err
	}
	var h uintptr
	rc := lib.openV2(path, &h, flags, 0)
	if rc != sqliteOK {
		code := rc
		msg := "unable to open database"
		if h != 0 {
			if ec := lib.errcode(h); ec != 0 {
				code = ec
			}
			msg = lib.errmsg(h)
			lib.close(h)
		}
		return nil, &sqliteError{code: code, msg: msg}
	}
	return &sqliteDB{lib: lib, handle: h}, nil
}

func (db *sqliteDB) close() error {
	if db == nil || db.handle == 0 {
		return nil
	}
	rc := db.lib.close(db.handle)
	db.handle = 0
	if rc != sqliteOK {
		return &sqliteError{code: rc, msg: "sqlite3_close failed"}
	}
	return nil
}

func (db *sqliteDB) exec(sql string) error {
	rc := db.lib.exec(db.handle, sql, 0, 0, nil)
	if rc != sqliteOK {
		return db.lastError(rc)
	}
	return nil
}

func (db *sqliteDB) lastInsertRowID() int64 {
	return db.lib.lastInsertRowID(db.handle)
}

func (db *sqliteDB) lastError(fallback int32) error {
	code := db.lib.errcode(db.handle)
	if code == 0 {
		code = fallback
	}
	return &sqliteError{code: code, msg: db.lib.errmsg(db.handle)}
}

type sqliteStmt struct {
	db   *sqliteDB
	stmt uintptr
}

func (db *sqliteDB) prepare(sql string, args ...any) (*sqliteStmt, error) {
	var st uintptr
	rc := db.lib.prepareV2(db.handle, sql, -1, &st, 0)
	if rc != sqliteOK {
		return nil, db.lastError(rc)
	}
	s := &sqliteStmt{db: db, stmt: st}
	for i, arg := range args {
		idx := int32(i + 1)
		var brc int32
		switch v := arg.(type) {
		case nil:
			brc = db.lib.bindNull(st, idx)
		case int:
			brc = db.lib.bindInt64(st, idx, int64(v))
		case int64:
			brc = db.lib.bindInt64(st, idx, v)
		case string:
			brc = db.lib.bindText(st, idx, v, int32(len(v)), sqliteTransient)
		case *string:
			if v == nil {
				brc = db.lib.bindNull(st, idx)
			} else {
				brc = db.lib.bindText(st, idx, *v, int32(len(*v)), sqliteTransient)
			}
		case []byte:
			if v == nil {
				brc = db.lib.bindNull(st, idx)
			} else if len(v) == 0 {
				brc = db.lib.bindBlob(st, idx, []byte{0}, 0, sqliteTransient)
			} else {
				brc = db.lib.bindBlob(st, idx, v, int32(len(v)), sqliteTransient)
			}
		default:
			s.finalize()
			return nil, fmt.Errorf("imessage: unsupported sqlite bind type %T", arg)
		}
		if brc != sqliteOK {
			err := db.lastError(brc)
			s.finalize()
			return nil, err
		}
	}
	return s, nil
}

func (s *sqliteStmt) step() (bool, error) {
	rc := s.db.lib.step(s.stmt)
	if rc == sqliteRow {
		return true, nil
	}
	if rc == sqliteDone {
		return false, nil
	}
	return false, s.db.lastError(rc)
}

func (s *sqliteStmt) isNull(col int32) bool {
	return s.db.lib.columnType(s.stmt, col) == sqliteNull
}

func (s *sqliteStmt) colInt64(col int32) int64 {
	return s.db.lib.columnInt64(s.stmt, col)
}

func (s *sqliteStmt) colText(col int32) (string, bool) {
	if s.isNull(col) {
		return "", false
	}
	ptr := s.db.lib.columnText(s.stmt, col)
	n := s.db.lib.columnBytes(s.stmt, col)
	if ptr == nil || n <= 0 {
		return "", true
	}
	return string(unsafe.Slice(ptr, int(n))), true
}

func (s *sqliteStmt) colBlob(col int32) []byte {
	if s.isNull(col) {
		return nil
	}
	ptr := s.db.lib.columnBlob(s.stmt, col)
	n := s.db.lib.columnBytes(s.stmt, col)
	if ptr == nil || n <= 0 {
		return []byte{}
	}
	out := make([]byte, int(n))
	copy(out, unsafe.Slice(ptr, int(n)))
	return out
}

func (s *sqliteStmt) finalize() {
	if s != nil && s.stmt != 0 {
		s.db.lib.finalize(s.stmt)
		s.stmt = 0
	}
}
