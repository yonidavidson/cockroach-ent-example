package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"

	"entgo.io/ent/dialect"
	_ "github.com/jackc/pgx/v4/stdlib"

	entsql "entgo.io/ent/dialect/sql"

	"github.com/yonidavidson/cockroachent/driver"
	"github.com/yonidavidson/cockroachent/ent"
	"github.com/yonidavidson/cockroachent/ent/account"
)

func main() {
	db, err := sql.Open("pgx", os.Getenv("COCKROACH_DSN"))
	if err != nil {
		log.Fatal(err)
	}
	drv := entsql.OpenDB(dialect.Postgres, db)
	cd := driver.New(drv)
	client := ent.NewClient(ent.Driver(cd))
	defer client.Close()
	ctx := context.Background()
	// run the auto migration tool.
	if err := client.Schema.Create(ctx); err != nil {
		log.Fatalf("failed creating schema resources: %v", err)
	}
	if _, err = CreateAccount(ctx, client); err != nil {
		log.Fatal(err)
	}
	if _, err = QueryAccount(ctx, client); err != nil {
		log.Fatal(err)
	}
}

func CreateAccount(ctx context.Context, client *ent.Client) (*ent.Account, error) {
	u, err := client.Account.
		Create().
		SetBalance(35).
		Save(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed creating account: %w", err)
	}
	log.Println("account was created: ", u)
	return u, nil
}

func QueryAccount(ctx context.Context, client *ent.Client) (*ent.Account, error) {
	u, err := client.Account.
		Query().
		Where(account.BalanceGT(20)).
		First(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed querying account: %w", err)
	}
	log.Println("account returned: ", u)
	return u, nil
}
