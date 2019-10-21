// mysqldb project mysqldb.go
package mysqldb

import (
	"context"
	"database/sql"
	"time"
)

type DBProxy struct {
	master            *DB
	slave             []*DB
	DbOperator        DBOperate
	dbReady           bool
	slaveCount        int
	scanSeconds       int
	dbcollectioncheck chan int
	activeCount       int
	roundRobin        int
}

var UINT_MAX int = ^int(0)

func init() {
	InitLog()
}

func CreateDBProxy() *DBProxy {
	var dbProxy *DBProxy = &DBProxy{
		slaveCount:  0,
		activeCount: 0,
		dbReady:     false,
		scanSeconds: 5,
		roundRobin:  0,
		slave:       make([]*DB, 10),
	}
	dbProxy.dbcollectioncheck = dbProxy.startListener()
	return dbProxy
}

func (dbProxy *DBProxy) CloseCollection() error {
	dbProxy.dbcollectioncheck <- 1
	var errorCode error = nil
	for i := 0; i < dbProxy.slaveCount; i++ {
		if err := dbProxy.slave[i].CloseCollection(); err != nil {
			errorCode = err
		}

	}
	if err := dbProxy.master.CloseCollection(); err != nil {
		errorCode = err
	}
	return errorCode
}

func (dbProxy *DBProxy) IsReady() bool {
	return dbProxy.dbReady
}

func (dbProxy *DBProxy) checkDBCollection() {
	Debug(time.Now().Format("2001-01-02 15:04:05"))
	if dbProxy.master.Ping() != nil {
		dbProxy.dbReady = false
	} else {
		dbProxy.dbReady = true
	}

	activeCount := 0
	for i := 0; i < dbProxy.slaveCount; i++ {
		if dbProxy.slave[i].Ping() == nil {
			activeCount = activeCount + 1
		}
	}
	dbProxy.activeCount = activeCount
}

func (dbProxy *DBProxy) startListener() chan int {
	done := make(chan int, 1)
	go func() {
		timer := time.NewTicker(time.Duration(dbProxy.scanSeconds) * time.Second)
		defer timer.Stop()
		for {
			select {
			case <-timer.C:
				dbProxy.checkDBCollection()
			case <-done:
				return
			}
		}
	}()
	return done
}

func (dbProxy *DBProxy) SetMasterDB(DbName string, DbConnectionUrl string, DbUserName string, DbPassword string, DbUrlParameter string) {
	dbProxy.master = CreateDBInstance(DbName, DbConnectionUrl, DbUserName, DbPassword, DbUrlParameter)
	if dbProxy.master != nil && dbProxy.master.dbReady {
		dbProxy.dbReady = true
	} else {
		dbProxy.dbReady = false
	}
}

func (dbProxy *DBProxy) AddSlaveDB(DbName string, DbConnectionUrl string, DbUserName string, DbPassword string, DbUrlParameter string) {
	var newDB *DB = CreateDBInstance(DbName, DbConnectionUrl, DbUserName, DbPassword, DbUrlParameter)
	dbProxy.addToSlaveDB(newDB)
}

func (dbProxy *DBProxy) addToSlaveDB(newDB *DB) {
	if dbProxy.slaveCount >= len(dbProxy.slave) {
		dbProxy.resizeSlaveDB()
	}
	dbProxy.slave[dbProxy.slaveCount] = newDB
	dbProxy.slaveCount = dbProxy.slaveCount + 1
}

func (dbProxy *DBProxy) resizeSlaveDB() {
	newArr := make([]*DB, len(dbProxy.slave)*2)
	for i := 0; i < len(dbProxy.slave); i++ {
		newArr[i] = dbProxy.slave[i]
	}
	dbProxy.slave = newArr
}

func (dbProxy *DBProxy) SetConnMaxLifetime(d time.Duration) {
	dbProxy.master.SetConnMaxLifetime(d)
	for i := 0; i < dbProxy.slaveCount; i++ {
		dbProxy.slave[i].SetConnMaxLifetime(d)
	}
}

func (dbProxy *DBProxy) SetMaxIdleConns(n int) {
	dbProxy.master.SetMaxIdleConns(n)
	for i := 0; i < dbProxy.slaveCount; i++ {
		dbProxy.slave[i].SetMaxIdleConns(n)
	}
}

func (dbProxy *DBProxy) SetMaxOpenConns(n int) {
	dbProxy.master.SetMaxOpenConns(n)
	for i := 0; i < dbProxy.slaveCount; i++ {
		dbProxy.slave[i].SetMaxOpenConns(n)
	}
}

func (dbProxy *DBProxy) getRoundRobin() *DB {
	if dbProxy.activeCount == 0 {
		return dbProxy.master
	}
	if dbProxy.roundRobin == UINT_MAX {
		dbProxy.roundRobin = 0
	}
	dbProxy.roundRobin++
	index := dbProxy.roundRobin % dbProxy.activeCount
	var selectDB *DB = nil
	j := 0
	for i := 0; i < dbProxy.slaveCount; i++ {
		if dbProxy.slave[i].dbReady {
			j++
			selectDB = dbProxy.slave[i]
		}
		if j == index+1 {
			return dbProxy.slave[i]
		}
	}
	if selectDB == nil {
		selectDB = dbProxy.master
	}
	return selectDB
}

func (dbProxy *DBProxy) Query(query string, args ...interface{}) (*sql.Rows, error) {
	return dbProxy.getRoundRobin().Query(query, args)
}

func (dbProxy *DBProxy) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return dbProxy.getRoundRobin().QueryContext(ctx, query, args)
}

func (dbProxy *DBProxy) QueryRowContext(ctx context.Context, query string, args ...interface{}) *sql.Row {
	return dbProxy.getRoundRobin().QueryRowContext(ctx, query, args)
}

func (dbProxy *DBProxy) QueryRow(query string, args ...interface{}) *sql.Row {
	return dbProxy.getRoundRobin().QueryRow(query, args)
}

func (dbProxy *DBProxy) Begin() (*sql.Tx, error) {
	return dbProxy.master.Begin()
}

func (dbProxy *DBProxy) BeginTx(ctx context.Context, opts *sql.TxOptions) (*sql.Tx, error) {
	return dbProxy.master.BeginTx(ctx, opts)
}

func (dbProxy *DBProxy) Exec(query string, args ...interface{}) (sql.Result, error) {
	return dbProxy.master.Exec(query, args)
}

func (dbProxy *DBProxy) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return dbProxy.master.ExecContext(ctx, query, args)
}

func (dbProxy *DBProxy) Prepare(query string) (*sql.Stmt, error) {
	return dbProxy.master.Prepare(query)
}

func (dbProxy *DBProxy) PrepareContext(ctx context.Context, query string) (*sql.Stmt, error) {
	return dbProxy.master.PrepareContext(ctx, query)
}
