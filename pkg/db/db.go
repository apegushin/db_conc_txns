package db

import (
	"fmt"
	"maps"
	"strings"
	"sync"
)

// ExecMultiStatementTxn executes multiple statements as a single transaction (txn)
// supported statements examples:
//  1. CREATE TABLE NAME
//  2. ADD_RECORD TABLE NAME COL1:VAL1,COL2:VAL2
//
// all statements are validated and applied atomically. if for any statement parsing or
// validation fails, none of the statements are applied. in case of no validation
// errors all changes are visible at once after the call to the function returns.
func (d *Database) ExecMultiStatementTxn(statements []string) error {
	if len(statements) == 0 {
		return nil
	}

	// create a multiStatementTxnDef from the txn statements. returns parsing error
	multiStatementTxnDef, err := newMultiStatementTxnDef(statements)
	if err != nil {
		return fmt.Errorf("error during initial statements parsing: %w", err)
	}

	// lock the DB and validate the txn definition. The DB needs to be at
	// least read-locked so that existing tables can be verified against
	// ADD_RECORD and CREATE TABLE requests.
	// Txn Definition determines if total txn change requires DB to be write-locked
	dbLevelLock(lock, d, multiStatementTxnDef)

	err = validate(multiStatementTxnDef, d)
	if err != nil {
		dbLevelLock(unlock, d, multiStatementTxnDef)
		return fmt.Errorf("error during transaction validation: %w", err)
	}

	// if DB write-lock is not required by the txn, while holding the DB read-lock,
	// acquire per-table write-locks for the add-records tables in the txn definition.
	perTableLocks(lock, d, multiStatementTxnDef)

	// with the DB and per-table locks acquired (if needed) proceed with txn execution
	execute(multiStatementTxnDef, d)

	// txn successfully completed. unlock per-table locks, then unlock DB lock
	perTableLocks(unlock, d, multiStatementTxnDef)
	dbLevelLock(unlock, d, multiStatementTxnDef)

	return nil
}

type Database struct {
	Name   string
	mu     sync.RWMutex
	tables map[string]*table
}

func NewDatabase(name string) *Database {
	return &Database{
		Name:   name,
		tables: make(map[string]*table),
	}
}

func (d *Database) String() string {
	res := fmt.Sprintf("DATABASE name: %s, number of tables: %d\n",
		d.Name, len(d.tables))
	for _, table := range d.tables {
		res += table.String()
	}
	return res
}

type table struct {
	mu      sync.RWMutex
	name    string
	records []*record
}

func newTable(tableName string) *table {
	return &table{
		name:    tableName,
		records: make([]*record, 0),
	}
}

func (t *table) String() string {
	res := fmt.Sprintf("\tTable name: %s, number of records: %d\n", t.name, len(t.records))
	for i, r := range t.records {
		recStr := fmt.Sprintf("\t\tRecord #%d: %s\n", i, r)
		res += recStr
	}
	return res
}

type record struct {
	cells map[string]string
}

func (r *record) String() string {
	var res string
	for c, v := range r.cells {
		res += fmt.Sprintf("%s:%s,", c, v)
	}
	return res
}

func newRecord(recordStr string) (*record, error) {
	if len(recordStr) == 0 {
		return nil, fmt.Errorf("record string can not be empty")
	}

	record := &record{
		cells: make(map[string]string),
	}
	for _, colValStr := range strings.Split(recordStr, ",") {
		colVal := strings.Split(colValStr, ":")
		if len(colVal) != 2 {
			return nil, fmt.Errorf("incorrect record string format: %s",
				recordStr)
		}
		record.cells[colVal[0]] = colVal[1]
	}
	return record, nil
}

type multiStatementTxnDef struct {
	tablesToCreate  *Set
	recordsToAppend map[string][]*record
}

// for CREATE TABLE requests build a set of unique table names to create
// for ADD_RECORD requests create records and map them to table names
// in case of any parsing errors, returns the error and the txn will be cancelled
func newMultiStatementTxnDef(statements []string) (*multiStatementTxnDef, error) {
	txnDef := &multiStatementTxnDef{
		tablesToCreate:  NewSet(),
		recordsToAppend: make(map[string][]*record),
	}

	for idx, statement := range statements {
		switch {
		case strings.HasPrefix(statement, "CREATE TABLE"):
			tableName, err := tableNameFromCreateStatement(statement)
			if err != nil {
				return nil, fmt.Errorf("error parsing statement #%d: %w", idx+1, err)
			}
			if txnDef.tablesToCreate.Contains(tableName) {
				return nil, fmt.Errorf("duplicate create table requests not allowed. table name: %s", tableName)
			}
			txnDef.tablesToCreate.Add(tableName)
		case strings.HasPrefix(statement, "ADD_RECORD TABLE"):
			tableName, record, err := tableNameAndRecordFromAddRecordStatement(statement)
			if err != nil {
				return nil, fmt.Errorf("error parsing statement #%d: %w", idx+1, err)
			}
			txnDef.recordsToAppend[tableName] = append(txnDef.recordsToAppend[tableName], record)
		default:
			// this will catch both empty and malformed statement strings
			return nil, fmt.Errorf("unsupported transaction statement #%d", idx+1)
		}
	}

	return txnDef, nil
}

// The entire DB is at least read-locked, which allows to run the following validation:
//  1. validate that tables to be created do not exist yet
//  2. validate that ADD_RECORD requests reference existing tables or tables to be created
func validate(m *multiStatementTxnDef, pdb *Database) error {
	for tableToCreate := range m.tablesToCreate.Items() {
		if pdb.tableExists(tableToCreate) {
			return fmt.Errorf("table named %s already exists", tableToCreate)
		}
	}

	for tableToAppendTo := range maps.Keys(m.recordsToAppend) {
		if !pdb.tableExists(tableToAppendTo) && !m.tablesToCreate.Contains(tableToAppendTo) {
			return fmt.Errorf("table named %s does not exist. can not append record to it",
				tableToAppendTo)
		}
	}

	return nil
}

// The entire DB is at least read-locked, which allows to run the following validation:
//  1. validate that tables to be created do not exist yet
//  2. validate that ADD_RECORD requests reference existing tables or tables to be created
func execute(m *multiStatementTxnDef, pdb *Database) {
	for tableToCreate := range m.tablesToCreate.Items() {
		pdb.addEmptyTable(tableToCreate)
	}

	for tableToAppendTo := range maps.Keys(m.recordsToAppend) {
		table := pdb.tables[tableToAppendTo]
		table.records = append(table.records, m.recordsToAppend[tableToAppendTo]...)
	}
}

// Example of create table transaction statement:
// "CREATE TABLE MyTable"
func tableNameFromCreateStatement(statement string) (string, error) {
	statementFields := strings.Fields(statement)
	if len(statementFields) != 3 {
		return "", fmt.Errorf("malformed create table statement")
	}

	return statementFields[2], nil
}

// Example of add-record transaction statement:
// "ADD_RECORD TABLE MyTable COL1:VAL1,COL2:VAL2"
func tableNameAndRecordFromAddRecordStatement(statement string) (string, *record, error) {
	statementFields := strings.Fields(statement)
	if len(statementFields) != 4 {
		return "", nil, fmt.Errorf("malformed add_record statement")
	}

	tableName := statementFields[2]
	if len(tableName) == 0 {
		return "", nil, fmt.Errorf("table name can not be an empty string")
	}

	recToAdd, err := newRecord(statementFields[3])
	if err != nil {
		return "", nil, fmt.Errorf("error creating new table record: %w", err)
	}

	return tableName, recToAdd, nil
}

// multi-statement txn defines the criteria for DB level writer vs read lock
func (m *multiStatementTxnDef) requiresDbWriteLocked() bool {
	return !m.tablesToCreate.IsEmpty()
}

func (d *Database) tableExists(tableName string) bool {
	_, ok := d.tables[tableName]
	return ok
}

func (d *Database) addEmptyTable(tableName string) {
	d.tables[tableName] = newTable(tableName)
}

func (d *Database) rLockDB() {
	d.mu.RLock()
	fmt.Printf("READ-LOCKED DB %s\n", d.Name)
}

func (d *Database) rUnlockDB() {
	d.mu.RUnlock()
	fmt.Printf("READ-UNLOCKED DB %s\n", d.Name)
}

func (d *Database) lockDB() {
	d.mu.Lock()
	fmt.Printf("WRITE-LOCKED DB %s\n", d.Name)
}

func (d *Database) unlockDB() {
	d.mu.Unlock()
	fmt.Printf("WRITE-UNLOCKED DB %s\n", d.Name)
}

func (t *table) rLockTable() {
	t.mu.RLock()
	fmt.Printf("READ-LOCKED table %s\n", t.name)
}

func (t *table) rUnlockTable() {
	t.mu.RUnlock()
	fmt.Printf("READ-UNLOCKED table %s\n", t.name)
}

func (t *table) lockTable() {
	t.mu.Lock()
	fmt.Printf("WRITE-LOCKED table %s\n", t.name)
}

func (t *table) unlockTable() {
	t.mu.Unlock()
	fmt.Printf("WRITE-UNLOCKED table %s\n", t.name)
}

type lockOp uint8

const (
	unlock lockOp = iota
	lock
)

func dbLevelLock(op lockOp, pdb *Database, multiStatementTxnDef *multiStatementTxnDef) {
	if multiStatementTxnDef.requiresDbWriteLocked() {
		switch op {
		case lock:
			pdb.lockDB()
		case unlock:
			pdb.unlockDB()
		}
	} else {
		switch op {
		case lock:
			pdb.rLockDB()
		case unlock:
			pdb.rUnlockDB()
		}
	}
}

func perTableLocks(op lockOp, pdb *Database, multiStatementTxnDef *multiStatementTxnDef) {
	if !multiStatementTxnDef.requiresDbWriteLocked() {
		for tableName := range maps.Keys(multiStatementTxnDef.recordsToAppend) {
			switch op {
			case lock:
				pdb.tables[tableName].lockTable()
			case unlock:
				pdb.tables[tableName].unlockTable()
			}
		}
	}
}
