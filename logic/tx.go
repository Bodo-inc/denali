package logic

import (
	"database/sql"

	"github.com/Bodo-inc/denali/models"
)

type Tx struct {
	Queries models.Querier
	Tx      *sql.Tx
	state   *State
}

func (s *State) WrapTx(f func(qtx *Tx) error) error {
	tx, err := s.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	qtx := &Tx{s.queries.WithTx(tx), tx, s}

	// Call the function
	if err := f(qtx); err != nil {
		return err
	}

	return tx.Commit()
}

func WrapTxRet[T any](s *State, f func(qtx *Tx) (T, error)) (T, error) {
	tx, err := s.db.Begin()
	if err != nil {
		return *new(T), err
	}
	defer tx.Rollback()
	qtx := &Tx{s.queries.WithTx(tx), tx, s}

	// Call the function
	out, err := f(qtx)
	if err != nil {
		return *new(T), err
	}

	return out, tx.Commit()
}
