package postgresql

import (
	"context"
	"fmt"
	"strings"

	"github.com/georgysavva/scany/v2/dbscan"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5"
	"github.com/rohitp934/guam/auth"
	"go.uber.org/zap"
)

var (
	logger                     *zap.SugaredLogger
	ESCAPED_USER_TABLE_NAME    string
	ESCAPED_KEY_TABLE_NAME     string
	ESCAPED_SESSION_TABLE_NAME string
)

type Tables struct {
	User    string
	Session string
	Key     string
}

type postgresAdapterImpl[T any] struct {
	ctx        context.Context
	db         *pgx.Conn
	tables     Tables
	userHelper HelperFunc[T]
	keyHelper  HelperFunc[auth.KeySchema]
}

type TestAdapter[T any] interface {
	GetUser(userId string) (*T, error)
	SetUser(user T, key *auth.KeySchema) error
}

func PostgresAdapter[S any, T any](
	ctx context.Context,
	db *pgx.Conn,
	tables Tables,
) TestAdapter[T] {
	ESCAPED_USER_TABLE_NAME = EscapeName(tables.User)
	ESCAPED_KEY_TABLE_NAME = EscapeName(tables.Key)
	ESCAPED_SESSION_TABLE_NAME = EscapeName(tables.Session)
	logger = zap.NewExample().Sugar()

	userHelper := CreatePreparedStatementHelper[T](func(index int) string {
		return fmt.Sprintf("$%d", index+1)
	})
	keyHelper := CreatePreparedStatementHelper[auth.KeySchema](func(index int) string {
		return fmt.Sprintf("$%d", index+1)
	})
	return &postgresAdapterImpl[T]{
		ctx:        ctx,
		db:         db,
		tables:     tables,
		userHelper: userHelper,
		keyHelper:  keyHelper,
	}
}

func insertIntoTable[T any](
	ctx context.Context,
	tx pgx.Tx,
	item T,
	tableName string,
	helper HelperFunc[T],
) error {
	fields, placeholders, args := helper(item)
	query := fmt.Sprintf(
		"INSERT INTO %s ( %s ) VALUES ( %s )",
		tableName,
		strings.Join(fields, ", "),
		strings.Join(placeholders, ", "),
	)
	_, err := tx.Exec(ctx, query, args...)
	if err != nil {
		return err
	}
	return nil
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
	logger.Infof("User: %+v\n", users)
	if users != nil {
		return &users[0], nil
	}
	return nil, nil
}

func (p *postgresAdapterImpl[T]) SetUser(user T, key *auth.KeySchema) error {
	if key == nil {
		userFields, userPlaceholders, userArgs := p.userHelper(user)
		query := fmt.Sprintf(
			"INSERT INTO %s ( %s ) VALUES ( %s )",
			ESCAPED_USER_TABLE_NAME,
			strings.Join(userFields, ", "),
			strings.Join(userPlaceholders, ", "),
		)

		_, err := p.db.Exec(p.ctx, query, userArgs...)
		if err != nil {
			logger.Errorln("Error while inserting into DB: ", err)
			return err
		}
		return nil
	}

	tx, err := p.db.Begin(p.ctx)
	if err != nil {
		return err
	}

	defer tx.Rollback(p.ctx)

	if err := insertIntoTable(p.ctx, tx, user, ESCAPED_USER_TABLE_NAME, p.userHelper); err != nil {
		return err
	}

	if err := insertIntoTable(p.ctx, tx, *key, ESCAPED_KEY_TABLE_NAME, p.keyHelper); err != nil {
		return err
	}

	return tx.Commit(p.ctx)
}
