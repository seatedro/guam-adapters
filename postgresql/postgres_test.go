// Write a test for the postgres package. The test should use pgx.
//

package postgresql

import (
	"context"
	"log"
	"os"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/joho/godotenv"
)

type User struct {
	ID       string
	Username string
}

func insert(ctx context.Context, conn *pgx.Conn) {
	// Create a new user.
	_, err := conn.Exec(context.Background(), "INSERT INTO auth_user (id, username) VALUES ($1, $2)", "1", "test@example.com")
	if err != nil {
		log.Fatal(err)
	}

	// Create a new session.
	_, err = conn.Exec(context.Background(), "INSERT INTO user_session (id, user_id, active_expires, idle_expires) VALUES ($1, $2, $3, $4)", "1", "1", 1702786038216, 1702786038216)
	if err != nil {
		log.Fatal(err)
	}

	// Create a new key.
	_, err = conn.Exec(context.Background(), "INSERT INTO user_key (id, user_id, hashed_password) VALUES ($1, $2, $3)", "1", "1", "s2:xflkjaasdfasdflkj")
	if err != nil {
		log.Fatal(err)
	}
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

func TestGetUser(t *testing.T) {
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
	defer conn.Close(context.Background())

	insert(ctx, conn)

	// Create a new adapter.
	adapter := getAdapter[any, User](ctx, conn)
	// Get the user.
	_, err = adapter.GetUser("1")
	if err != nil {
		log.Fatal(err)
	}

	delete(ctx, conn)
}
