package mysqldb

import (
	"context"
	"database/sql"
	"time"
)

type DB struct {
	dbConnectionUrl string
	dbUserName      string
	dbPassword      string
	dbUrlParameter  string
	dbName          string
	dbInstance      *sql.DB
	dbReady         bool
	duration        time.Duration
	maxIdleConns    int
	maxOpenConns    int
}

func init() {
	InitLog()
}

func CreateDBInstance(DbName string, DbConnectionUrl string, DbUserName string, DbPassword string, DbUrlParameter string) *DB {
	var db *DB = &DB{
		dbName:          DbName,
		dbConnectionUrl: DbConnectionUrl,
		dbUserName:      DbUserName,
		dbPassword:      DbPassword,
		dbUrlParameter:  DbUrlParameter,
		dbInstance:      nil,
		dbReady:         false,
	}
	db.ConnectDB()
	return db
}

func (db *DB) ConnectDB() error {
	var urlParameter = "?"
	var connectionUrl = ""
	if len(db.dbUrlParameter) > 0 {
		urlParameter += db.dbUrlParameter + "&"
	}
	connectionUrl = db.dbUserName + ":" + db.dbPassword + "@" + db.dbConnectionUrl + "/" + db.dbName + urlParameter + "parseTime=true"
	Debug(connectionUrl)
	var err error = nil
	db.dbInstance, err = sql.Open("mysql", connectionUrl)
	if err != nil {
		db.dbReady = false
		Error("Mysql Connection Error ")
	} else {
		db.dbReady = true
	}
	return err
}

func (db *DB) Ping() error {
	if err := db.dbInstance.Ping(); err != nil {
		return err
	} else {
		db.dbReady = true
	}
	return nil
}

func (db *DB) PingContext(ctx context.Context) error {
	if err := db.dbInstance.PingContext(ctx); err != nil {
		return err
	} else {
		db.dbReady = true
	}
	return nil
}

func (db *DB) CloseCollection() error {
	return db.dbInstance.Close()
}

func (db *DB) SetConnMaxLifetime(d time.Duration) {
	db.duration = d
	db.dbInstance.SetConnMaxLifetime(db.duration)
}

func (db *DB) SetMaxIdleConns(d int) {
	db.maxIdleConns = d
	db.dbInstance.SetMaxIdleConns(db.maxIdleConns)
}

func (db *DB) SetMaxOpenConns(d int) {
	db.maxOpenConns = d
	db.dbInstance.SetMaxOpenConns(db.maxOpenConns)
}

func (db *DB) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return db.dbInstance.Query(query, args)
}

func (db *DB) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return db.dbInstance.QueryContext(ctx, query, args)
}

func (db *DB) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return db.dbInstance.QueryRowContext(ctx, query, args)
}

func (db *DB) QueryRow(query string, args ...interface{}) *sql.Row {
	return db.dbInstance.QueryRow(query, args)
}

func (db *DB) Begin() (*sql.Tx, error) {
	return db.dbInstance.Begin()
}

func (db *DB) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return db.dbInstance.BeginTx(ctx, opts)
}

func (db *DB) Exec(query string, args ...interface{}) (sql.Result, error) {
	return db.dbInstance.Exec(query, args)
}

func (db *DB) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return db.dbInstance.ExecContext(ctx, query, args)
}

func (db *DB) Prepare(query string) (*sql.Stmt, error) {
	return db.dbInstance.Prepare(query)
}

func (db *DB) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return db.dbInstance.PrepareContext(ctx, query)
}
