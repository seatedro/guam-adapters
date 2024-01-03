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

type postgresAdapterImpl[U any, S any] struct {
	ctx           context.Context
	db            *pgx.Conn
	userHelper    HelperFunc[U]
	keyHelper     HelperFunc[auth.KeySchema]
	sessionHelper HelperFunc[S]
	tables        Tables
}

type TestAdapter[U any, S any] interface {
	GetUser(userId string) (*U, error)
	SetUser(user U, key *auth.KeySchema) error
	DeleteUser(userId string) error
	UpdateUser(userId string, partialUser U) error
	GetSession(sessionId string) (*S, error)
	GetSessionByUserId(userId string) (*S, error)
	SetSession(session S) error
	DeleteSession(sessionId string) error
	DeleteSessionByUserId(userId string) error
	UpdateSession(sessionId string, partialSession map[string]any) error
}

func PostgresAdapter[U any, S any](
	ctx context.Context,
	db *pgx.Conn,
	tables Tables,
) TestAdapter[U, S] {
	ESCAPED_USER_TABLE_NAME = EscapeName(tables.User)
	ESCAPED_KEY_TABLE_NAME = EscapeName(tables.Key)
	ESCAPED_SESSION_TABLE_NAME = EscapeName(tables.Session)
	logger = zap.NewExample().Sugar()

	userHelper := CreatePreparedStatementHelper[U](func(index int) string {
		return fmt.Sprintf("$%d", index+1)
	})
	keyHelper := CreatePreparedStatementHelper[auth.KeySchema](func(index int) string {
		return fmt.Sprintf("$%d", index+1)
	})
	sessionHelper := CreatePreparedStatementHelper[S](func(index int) string {
		return fmt.Sprintf("$%d", index+1)
	})
	return &postgresAdapterImpl[U, S]{
		ctx:           ctx,
		db:            db,
		tables:        tables,
		userHelper:    userHelper,
		keyHelper:     keyHelper,
		sessionHelper: sessionHelper,
	}
}

func insertIntoTable[U any](
	ctx context.Context,
	tx pgx.Tx,
	item U,
	tableName string,
	helper HelperFunc[U],
) error {
	fields, placeholders, args := helper(item)
	logger.Debugln("Fields: ", fields)
	logger.Debugln("Placeholders: ", placeholders)
	logger.Debugln("Args: ", args)
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

func (p *postgresAdapterImpl[U, S]) GetUser(userId string) (*U, error) {
	var users []U
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = $1", ESCAPED_USER_TABLE_NAME)
	logger.Debugln("Query: ", query)
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
	logger.Debugf("User: %+v\n", users)
	if users != nil {
		return &users[0], nil
	}
	return nil, nil
}

func (p *postgresAdapterImpl[U, S]) SetUser(user U, key *auth.KeySchema) error {
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

func (p *postgresAdapterImpl[U, S]) DeleteUser(userId string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", ESCAPED_USER_TABLE_NAME)

	_, err := p.db.Exec(p.ctx, query, userId)
	if err != nil {
		logger.Errorln("Error while deleting user: ", err)
		return err
	}
	return nil
}

func (p *postgresAdapterImpl[U, S]) UpdateUser(userId string, partialUser U) error {
	userFields, userPlaceholders, userArgs := p.userHelper(partialUser)
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE id = $%d",
		ESCAPED_USER_TABLE_NAME,
		GetSetArgs(userFields, userPlaceholders),
		len(userArgs)+1,
	)

	_, err := p.db.Exec(p.ctx, query, append(userArgs, userId)...)
	if err != nil {
		logger.Errorln("Error while updating user: ", err)
		return err
	}
	return nil
}

func (p *postgresAdapterImpl[U, S]) GetSession(sessionId string) (*S, error) {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil, nil
	}
	var sessions []S
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = $1", ESCAPED_SESSION_TABLE_NAME)
	logger.Debugln("Query: ", query)
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

	scan.Select(p.ctx, p.db, &sessions, query, sessionId)
	logger.Debugf("Sessions: %+v\n", sessions)
	if sessions != nil {
		return &sessions[0], nil
	}
	return nil, nil
}

func (p *postgresAdapterImpl[U, S]) GetSessionByUserId(userId string) (*S, error) {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil, nil
	}
	var sessions []S
	query := fmt.Sprintf("SELECT * FROM %s WHERE user_id = $1", ESCAPED_SESSION_TABLE_NAME)
	logger.Debugln("Query: ", query)
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

	scan.Select(p.ctx, p.db, &sessions, query, userId)
	logger.Debugf("Sessions: %+v\n", sessions)
	if sessions != nil {
		return &sessions[0], nil
	}
	return nil, nil
}

func (p *postgresAdapterImpl[U, S]) SetSession(session S) error {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil
	}
	sessionFields, sessionPlaceholders, sessionArgs := p.sessionHelper(session)
	query := fmt.Sprintf(
		"INSERT INTO %s ( %s ) VALUES ( %s )",
		ESCAPED_SESSION_TABLE_NAME,
		strings.Join(sessionFields, ", "),
		strings.Join(sessionPlaceholders, ", "),
	)

	_, err := p.db.Exec(p.ctx, query, sessionArgs...)
	if err != nil {
		logger.Errorln("Error while inserting into DB: ", err)
		return err
	}

	return nil
}

func (p *postgresAdapterImpl[U, S]) DeleteSession(sessionId string) error {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", ESCAPED_SESSION_TABLE_NAME)

	_, err := p.db.Exec(p.ctx, query, sessionId)
	if err != nil {
		logger.Errorln("Error while deleting session: ", err)
		return err
	}

	return nil
}

func (p *postgresAdapterImpl[U, S]) DeleteSessionByUserId(userId string) error {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil
	}
	query := fmt.Sprintf("DELETE FROM %s WHERE user_id = $1", ESCAPED_SESSION_TABLE_NAME)

	_, err := p.db.Exec(p.ctx, query, userId)
	if err != nil {
		logger.Errorln("Error while deleting session: ", err)
		return err
	}

	return nil
}

func (p *postgresAdapterImpl[U, S]) UpdateSession(
	sessionId string,
	partialSession map[string]any,
) error {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil
	}
	var sessionFields []string
	var sessionPlaceholders []string
	var sessionArgs []interface{}
	i := 0
	for key, value := range partialSession {
		sessionFields = append(sessionFields, EscapeName(key))
		sessionPlaceholders = append(sessionPlaceholders, fmt.Sprintf("$%d", i+1))
		sessionArgs = append(sessionArgs, value)
		i++
	}
	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE id = $%d",
		ESCAPED_SESSION_TABLE_NAME,
		GetSetArgs(sessionFields, sessionPlaceholders),
		len(sessionArgs)+1,
	)

	_, err := p.db.Exec(p.ctx, query, append(sessionArgs, sessionId)...)
	if err != nil {
		logger.Errorln("Error while updating session: ", err)
		return err
	}
	return nil
}
