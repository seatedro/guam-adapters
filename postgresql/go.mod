module github.com/seatedro/guam-adapters/postgresql

go 1.21.0

require (
	github.com/georgysavva/scany/v2 v2.0.0
	github.com/jackc/pgx/v5 v5.5.1
	github.com/joho/godotenv v1.5.1
	github.com/seatedro/guam v0.0.3
	go.uber.org/zap v1.26.0
)

require (
	github.com/jackc/pgpassfile v1.0.0 // indirect
	github.com/jackc/pgservicefile v0.0.0-20221227161230-091c0ba34f0a // indirect
	github.com/jackc/puddle/v2 v2.2.1 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/crypto v0.17.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/text v0.14.0 // indirect
)

replace github.com/seatedro/guam => ../../guam
