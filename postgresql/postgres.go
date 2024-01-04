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

type postgresAdapterImpl struct {
	ctx           context.Context
	db            *pgx.Conn
	userHelper    HelperFunc[auth.UserSchema]
	keyHelper     HelperFunc[auth.KeySchema]
	sessionHelper HelperFunc[auth.SessionSchema]
	tables        Tables
}

func PostgresAdapter(
	ctx context.Context,
	db *pgx.Conn,
	tables Tables,
	debugMode bool,
) auth.AdapterWithGetter {
	ESCAPED_USER_TABLE_NAME = EscapeName(tables.User)
	ESCAPED_KEY_TABLE_NAME = EscapeName(tables.Key)
	ESCAPED_SESSION_TABLE_NAME = EscapeName(tables.Session)
	if debugMode {
		l, err := zap.NewDevelopment()
		if err != nil {
			logger = zap.NewNop().Sugar()
		}
		logger = l.Sugar()
	} else {
		l, err := zap.NewProduction(zap.IncreaseLevel(zap.ErrorLevel))
		if err != nil {
			logger = zap.NewNop().Sugar()
		}
		logger = l.Sugar()
	}

	userHelper := CreatePreparedStatementHelper[auth.UserSchema](func(index int) string {
		return fmt.Sprintf("$%d", index+1)
	})
	keyHelper := CreatePreparedStatementHelper[auth.KeySchema](func(index int) string {
		return fmt.Sprintf("$%d", index+1)
	})
	sessionHelper := CreatePreparedStatementHelper[auth.SessionSchema](func(index int) string {
		return fmt.Sprintf("$%d", index+1)
	})
	return &postgresAdapterImpl{
		ctx:           ctx,
		db:            db,
		tables:        tables,
		userHelper:    userHelper,
		keyHelper:     keyHelper,
		sessionHelper: sessionHelper,
	}
}

func insertIntoTable(
	ctx context.Context,
	tx pgx.Tx,
	tableName string,
	fields []string,
	placeholders []string,
	args []any,
) error {
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

func (p *postgresAdapterImpl) GetUser(
	userId string,
) (*auth.UserSchema, error) {
	var users []auth.UserSchema
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

func (p *postgresAdapterImpl) SetUser(user auth.UserSchema, key *auth.KeySchema) error {
	if key == nil {
		userFields, userPlaceholders, userArgs := p.userHelper(user)

		// If struct has Attributes field, append it to args
		i := len(userArgs)
		for key, val := range user.Attributes {
			userFields = append(userFields, EscapeName(key))
			userPlaceholders = append(userPlaceholders, fmt.Sprintf("$%d", i+1))
			userArgs = append(userArgs, val)
			i++
		}

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

	userFields, userPlaceholders, userArgs := p.userHelper(user)

	// If struct has Attributes field, append it to args
	i := len(userArgs)
	for key, val := range user.Attributes {
		userFields = append(userFields, EscapeName(key))
		userPlaceholders = append(userPlaceholders, fmt.Sprintf("$%d", i+1))
		userArgs = append(userArgs, val)
		i++
	}

	if err := insertIntoTable(p.ctx, tx, ESCAPED_USER_TABLE_NAME, userFields, userPlaceholders, userArgs); err != nil {
		return err
	}

	keyFields, keyPlaceholders, keyArgs := p.keyHelper(*key)

	if err := insertIntoTable(p.ctx, tx, ESCAPED_KEY_TABLE_NAME, keyFields, keyPlaceholders, keyArgs); err != nil {
		return err
	}

	return tx.Commit(p.ctx)
}

func (p *postgresAdapterImpl) DeleteUser(userId string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", ESCAPED_USER_TABLE_NAME)

	_, err := p.db.Exec(p.ctx, query, userId)
	if err != nil {
		logger.Errorln("Error while deleting user: ", err)
		return err
	}
	return nil
}

func (p *postgresAdapterImpl) UpdateUser(
	userId string,
	partialUser map[string]any,
) error {
	var userFields []string
	var userPlaceholders []string
	var userArgs []interface{}
	i := 0
	for key, value := range partialUser {
		userFields = append(userFields, EscapeName(key))
		userPlaceholders = append(userPlaceholders, fmt.Sprintf("$%d", i+1))
		userArgs = append(userArgs, value)
		i++
	}
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

func (p *postgresAdapterImpl) GetSession(
	sessionId string,
) (*auth.SessionSchema, error) {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil, nil
	}
	var sessions []auth.SessionSchema
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

func (p *postgresAdapterImpl) GetSessionsByUserId(
	userId string,
) ([]auth.SessionSchema, error) {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil, nil
	}
	var sessions []auth.SessionSchema
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
		return sessions, nil
	}
	return nil, nil
}

func (p *postgresAdapterImpl) SetSession(
	session auth.SessionSchema,
) error {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil
	}
	sessionFields, sessionPlaceholders, sessionArgs := p.sessionHelper(session)

	// If struct has Attributes field, append it to args
	i := len(sessionArgs)
	for key, val := range session.Attributes {
		sessionFields = append(sessionFields, EscapeName(key))
		sessionPlaceholders = append(sessionPlaceholders, fmt.Sprintf("$%d", i+1))
		sessionArgs = append(sessionArgs, val)
		i++
	}

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

func (p *postgresAdapterImpl) DeleteSession(
	sessionId string,
) error {
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

func (p *postgresAdapterImpl) DeleteSessionsByUserId(
	userId string,
) error {
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

func (p *postgresAdapterImpl) UpdateSession(
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

func (p *postgresAdapterImpl) GetKey(keyId string) (*auth.KeySchema, error) {
	var keys []auth.KeySchema
	query := fmt.Sprintf("SELECT * FROM %s WHERE id = $1", ESCAPED_KEY_TABLE_NAME)

	logger.Debugln("Query: ", query)
	pgxscan.Select(p.ctx, p.db, &keys, query, keyId)

	logger.Debugf("Keys: %+v\n", keys)
	if keys != nil {
		return &keys[0], nil
	}

	return nil, nil
}

func (p *postgresAdapterImpl) GetKeysByUserId(userId string) ([]auth.KeySchema, error) {
	var keys []auth.KeySchema
	query := fmt.Sprintf("SELECT * FROM %s WHERE user_id = $1", ESCAPED_KEY_TABLE_NAME)

	logger.Debugln("Query: ", query)
	pgxscan.Select(p.ctx, p.db, &keys, query, userId)

	logger.Debugf("Keys: %+v\n", keys)

	return keys, nil
}

func (p *postgresAdapterImpl) SetKey(key auth.KeySchema) error {
	keyFields, keyPlaceholders, keyValues := p.keyHelper(key)

	query := fmt.Sprintf(
		"INSERT INTO %s ( %s ) VALUES ( %s )",
		ESCAPED_KEY_TABLE_NAME,
		strings.Join(keyFields, ", "),
		strings.Join(keyPlaceholders, ", "),
	)

	_, err := p.db.Exec(p.ctx, query, keyValues...)
	if err != nil {
		logger.Errorln("Error while inserting into Keys table: ", err)
		return err
	}

	return nil
}

func (p *postgresAdapterImpl) UpdateKey(keyId string, partialKey map[string]any) error {
	var keyFields []string
	var keyPlaceholders []string
	var keyValues []any

	i := 0
	for k, v := range partialKey {
		keyFields = append(keyFields, k)
		keyPlaceholders = append(keyPlaceholders, fmt.Sprintf("$%d", i+1))
		keyValues = append(keyValues, v)
	}

	query := fmt.Sprintf(
		"UPDATE %s SET %s WHERE id = $%d",
		ESCAPED_KEY_TABLE_NAME,
		GetSetArgs(keyFields, keyPlaceholders),
		len(keyFields)+1,
	)

	_, err := p.db.Exec(p.ctx, query, append(keyValues, keyId)...)
	if err != nil {
		logger.Errorln("Error while updating Key table: ", err)
		return err
	}

	return nil
}

func (p *postgresAdapterImpl) DeleteKey(keyId string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", ESCAPED_KEY_TABLE_NAME)

	_, err := p.db.Exec(p.ctx, query, keyId)
	if err != nil {
		logger.Errorln("Error while deleteing from Key table: ", err)
		return err
	}

	return nil
}

func (p *postgresAdapterImpl) DeleteKeysByUserId(userId string) error {
	query := fmt.Sprintf("DELETE FROM %s WHERE user_id = $1", ESCAPED_KEY_TABLE_NAME)

	_, err := p.db.Exec(p.ctx, query, userId)
	if err != nil {
		logger.Errorln("Error while deleteing from Key table: ", err)
		return err
	}

	return nil
}

func (p *postgresAdapterImpl) GetSessionAndUser(
	sessionId string,
) (*auth.SessionSchema, *auth.UserJoinSessionSchema, error) {
	if ESCAPED_SESSION_TABLE_NAME == "" {
		return nil, nil, nil
	}

	session, err := p.GetSession(sessionId)
	if err != nil {
		logger.Errorln("Error while fetching Session: ", err)
		return nil, nil, err
	}

	var result []auth.UserJoinSessionSchema
	query := fmt.Sprintf(
		"SELECT %s.*, %s.id AS __session_id FROM %s INNER JOIN %s ON %s.id = %s.user_id WHERE %s.id = $1",
		ESCAPED_USER_TABLE_NAME,
		ESCAPED_SESSION_TABLE_NAME,
		ESCAPED_SESSION_TABLE_NAME,
		ESCAPED_USER_TABLE_NAME,
		ESCAPED_USER_TABLE_NAME,
		ESCAPED_SESSION_TABLE_NAME,
		ESCAPED_SESSION_TABLE_NAME,
	)

	logger.Debugln("Query: ", query)
	api, err := pgxscan.NewDBScanAPI(dbscan.WithAllowUnknownColumns(true))
	if err != nil {
		logger.Errorln("Error: ", err)
		return nil, nil, err
	}
	scan, err := pgxscan.NewAPI(api)
	if err != nil {
		logger.Errorln("Error: ", err)
		return nil, nil, err
	}

	scan.Select(p.ctx, p.db, &result, query, sessionId)

	logger.Debugf("Result: %+v\n", result[0])

	if result != nil {
		return session, &result[0], nil
	}
	return nil, nil, nil
}
