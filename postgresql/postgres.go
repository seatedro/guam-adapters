package postgresql

import (
	"context"
	"fmt"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"
)

var logger *zap.SugaredLogger
var ESCAPED_USER_TABLE_NAME, ESCAPED_KEY_TABLE_NAME, ESCAPED_SESSION_TABLE_NAME string

type Tables struct {
	User    string
	Session string
	Key     string
}

type postgresAdapterImpl[T any] struct {
	ctx    context.Context
	db     *pgx.Conn
	tables Tables
}

type TestAdapter[T any] interface {
	GetUser(userId string) (*T, error)
}

func PostgresAdapter[S any, T any](ctx context.Context, db *pgx.Conn, tables Tables) TestAdapter[T] {
	ESCAPED_USER_TABLE_NAME = EscapeName(tables.User)
	ESCAPED_KEY_TABLE_NAME = EscapeName(tables.Key)
	ESCAPED_SESSION_TABLE_NAME = EscapeName(tables.Session)
	logger = zap.NewExample().Sugar()
	return &postgresAdapterImpl[T]{
		ctx:    ctx,
		db:     db,
		tables: tables,
	}
}

func (p *postgresAdapterImpl[T]) GetUser(userId string) (*T, error) {
	var users []T
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = $1", ESCAPED_USER_TABLE_NAME)
	logger.Infoln("Query: ", query)
	api, err := pgxscan.NewDBScanAPI(dbscan.WithAllowUnknownColumns(true))
	if err != nil {
		logger.Errorln("Error: ", err)
		return nil, err
	}
	scan, err := pgxscan.NewAPI(api)
	if err != nil {
		logger.Errorln("Error: ", err)
		return nil, err
	}

	scan.Select(p.ctx, p.db, &users, query, userId)
	logger.Infof("User: %+v\n", users[0])
	return &users[0], nil
}
