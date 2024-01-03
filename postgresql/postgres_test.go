// Write a test for the postgres package. The test should use pgx.
//

package postgresql

import (
	"context"
	"log"
	"math/rand"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
	"github.com/rohitp934/guam/auth"
	"github.com/rohitp934/guam/utils"
)

type User struct {
	ID       string `db:"id"`
	Username string `db:"username"`
}

func insert(ctx context.Context, conn *pgx.Conn) string {
	// Create a new user.
	userId := utils.GenerateRandomString(5, "")
	username := utils.GenerateRandomString(6, "")
	_, err := conn.Exec(
		context.Background(),
		"INSERT INTO auth_user (id, username) VALUES ($1, $2)",
		userId,
		username,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new session.
	sessionId := utils.GenerateRandomString(5, "")
	active_expires := rand.Int63n(1000000000000)
	idle_expires := rand.Int63n(1000000000000)
	_, err = conn.Exec(
		context.Background(),
		"INSERT INTO user_session (id, user_id, active_expires, idle_expires) VALUES ($1, $2, $3, $4)",
		sessionId,
		userId,
		active_expires,
		idle_expires,
	)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new key.
	keyId := utils.GenerateRandomString(5, "")
	hashedPassword := utils.GenerateScryptHash(utils.GenerateRandomString(6, ""))
	_, err = conn.Exec(
		context.Background(),
		"INSERT INTO user_key (id, user_id, hashed_password) VALUES ($1, $2, $3)",
		keyId,
		userId,
		hashedPassword,
	)
	if err != nil {
		log.Fatal(err)
	}

	return userId
}

func getAdapter[S any, T any](ctx context.Context, conn *pgx.Conn) TestAdapter[T] {
	return PostgresAdapter[S, T](ctx, conn, Tables{
		User:    "auth_user",
		Session: "user_session",
		Key:     "user_key",
	})
}

func delete(ctx context.Context, conn *pgx.Conn) {
	// Delete all rows from the tables.

	_, err := conn.Exec(context.Background(), "DELETE FROM user_session")
	if err != nil {
		log.Fatal(err)
	}

	_, err = conn.Exec(context.Background(), "DELETE FROM user_key")
	if err != nil {
		log.Fatal(err)
	}

	_, err = conn.Exec(context.Background(), "DELETE FROM auth_user")
	if err != nil {
		log.Fatal(err)
	}
}

func setup[T any]() (context.Context, *pgx.Conn, TestAdapter[T]) {
	err := godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	// Connect to the "postgres" database.
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}

	// Create a new adapter.
	adapter := getAdapter[any, T](ctx, conn)

	return ctx, conn, adapter
}

func TestGetUser(t *testing.T) {
	ctx, conn, adapter := setup[User]()
	userId := insert(ctx, conn)

	defer conn.Close(ctx)

	// Get the user.
	_, err := adapter.GetUser(userId)
	if err != nil {
		log.Fatal(err)
	}

	delete(ctx, conn)
}

func createUser(adapter TestAdapter[User], withKey bool) string {
	// Set the user.
	var key *auth.KeySchema = nil
	userId := utils.GenerateRandomString(5, "")
	user := User{
		ID:       userId,
		Username: utils.GenerateRandomString(6, ""),
	}
	if withKey {
		hashedPassword := utils.GenerateScryptHash(utils.GenerateRandomString(6, ""))
		key = &auth.KeySchema{
			ID:             utils.GenerateRandomString(5, ""),
			UserID:         userId,
			HashedPassword: &hashedPassword,
		}
	}
	err := adapter.SetUser(user, key)
	if err != nil {
		log.Fatal(err)
	}

	return userId
}

func TestSetUser(t *testing.T) {
	ctx, conn, adapter := setup[User]()
	defer conn.Close(ctx)

	_ = createUser(adapter, false)

	delete(ctx, conn)
}

func TestSetUserWithKey(t *testing.T) {
	ctx, conn, adapter := setup[User]()
	defer conn.Close(ctx)

	_ = createUser(adapter, true)

	delete(ctx, conn)
}

func TestDeleteUser(t *testing.T) {
	ctx, conn, adapter := setup[User]()
	defer conn.Close(ctx)

	userId := createUser(adapter, false)
	// Delete the user.
	err := adapter.DeleteUser(userId)
	if err != nil {
		log.Fatal(err)
	}

	// Try to get the user.
	user, err := adapter.GetUser(userId)
	if err != nil || user != nil {
		log.Fatal(err)
	}

	delete(ctx, conn)
}

func TestUpdateUser(t *testing.T) {
	ctx, conn, adapter := setup[User]()
	defer conn.Close(ctx)

	userId := createUser(adapter, false)

	// Update the user.
	partialUser := User{
		Username: utils.GenerateRandomString(5, ""),
	}

	err := adapter.UpdateUser(userId, partialUser)
	if err != nil {
		log.Fatal(err)
	}

	delete(ctx, conn)
}
