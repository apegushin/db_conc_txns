package main

import (
	"fmt"
	"sync"

	"github.com/apegushin/db_conc_txns/pkg/db"
)

func main() {
	db := db.NewDatabase("MyDB")
	fmt.Printf("Database created:\n%s", db)
	sequentialTxns := [][]string{
		{
			"CREATE TABLE Table1",
			"CREATE TABLE Table2",
			"CREATE TABLE Table3",
		},
		{
			"ADD_RECORD TABLE Table1 NAME:Name1,EMAIL:Email1",
			"ADD_RECORD TABLE Table2 NAME:Name2,EMAIL:Email2",
			"ADD_RECORD TABLE Table3 NAME:Name3,EMAIL:Email3",
			"ADD_RECORD TABLE Table1 NAME:Name11,EMAIL:Email11",
			"ADD_RECORD TABLE Table2 NAME:Name22,EMAIL:Email22",
			"ADD_RECORD TABLE Table3 NAME:Name33,EMAIL:Email33",
		},
		{
			"CREATE TABLE Table4",
			"ADD_RECORD TABLE Table1 NAME:Name111,EMAIL:Email111",
			"ADD_RECORD TABLE Table2 NAME:Name222,EMAIL:Email222",
			"ADD_RECORD TABLE Table3 NAME:Name333,EMAIL:Email333",
			"ADD_RECORD TABLE Table4 NAME:Name4,EMAIL:Email4",
		},
	}
	concurrentTxns := [][]string{
		{
			"ADD_RECORD TABLE Table1 NAME:Name1111,EMAIL:Email1111",
			"ADD_RECORD TABLE Table2 NAME:Name2222,EMAIL:Email2222",
			"ADD_RECORD TABLE Table3 NAME:Name3333,EMAIL:Email3333",
			"ADD_RECORD TABLE Table4 NAME:Name44,EMAIL:Email44",
		},
		{
			"CREATE TABLE Table5",
			"ADD_RECORD TABLE Table3 NAME:Name33333,EMAIL:Email33333",
			"ADD_RECORD TABLE Table4 NAME:Name444,EMAIL:Email444",
			"ADD_RECORD TABLE Table5 NAME:Name5,EMAIL:Email5",
		},
		{
			"CREATE TABLE Table6",
			"ADD_RECORD TABLE Table4 NAME:Name4444,EMAIL:Email4444",
			"ADD_RECORD TABLE Table6 NAME:Name6,EMAIL:Email6",
		},
	}
	for idx, seqTxn := range sequentialTxns {
		err := db.ExecMultiStatementTxn(seqTxn)
		if err != nil {
			fmt.Printf("Seq txn #%d failed with error: %v", idx+1, err)
		} else {
			fmt.Printf("Seq txn #%d succeeded. Database after txn:\n%s", idx+1, db)
		}
	}
	errChan := make(chan error, len(concurrentTxns))
	go func() {
		var wg sync.WaitGroup
		for _, concTxn := range concurrentTxns {
			wg.Go(func() {
				err := db.ExecMultiStatementTxn(concTxn)
				if err != nil {
					errChan <- err
				}
			})
		}
		wg.Wait()
		close(errChan)
	}()
	for e := range errChan {
		fmt.Println(e)
	}
	fmt.Printf("Database after all sequential and concurrent txns:\n%s", db)
}
