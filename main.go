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
	"github.com/yonidavidson/cockroachent/ent/user"
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
	// delete old accounts
	if _, err := client.Account.Delete().Exec(ctx); err != nil {
		log.Fatal(err)
	}
	// delete old users
	if _, err := client.User.Delete().Exec(ctx); err != nil {
		log.Fatal(err)
	}
	accountA, err := CreateAccount(ctx, client, 50)
	if err != nil {
		log.Fatal(err)
	}
	accountB, err := CreateAccount(ctx, client, 50)
	if err != nil {
		log.Fatal(err)
	}
	userA, err := CreateUser(ctx, client, "Yoni")
	if err != nil {
		log.Fatal(err)
	}
	userB, err := CreateUser(ctx, client, "Amit")
	if err != nil {
		log.Fatal(err)
	}
	if err := client.Account.UpdateOne(accountA).SetOwner(userA).Exec(ctx); err != nil {
		log.Fatal(err)
	}
	if err := client.Account.UpdateOne(accountB).SetOwner(userB).Exec(ctx); err != nil {
		log.Fatal(err)
	}

	// get a transactional client
	tx, err := client.Tx(ctx)
	if err != nil {
		log.Fatal(err)
	}
	if err := tx.Account.UpdateOne(accountA).AddBalance(10).Exec(ctx); err != nil {
		log.Fatal(err)
	}
	if err := tx.Account.UpdateOne(accountB).AddBalance(-10).Exec(ctx); err != nil {
		log.Fatal(err)
	}
	if err != tx.Commit() {
		log.Fatal(err)
	}
	// end of transaction
	accountA, err = QueryAccount(ctx, client, "Yoni")
	if err != nil {
		log.Fatal(err)
	}
	accountB, err = QueryAccount(ctx, client, "Amit")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("Yoni's Account:", accountA)
	fmt.Println("Amit's Account:", accountB)

}

func CreateAccount(ctx context.Context, client *ent.Client, amount int) (*ent.Account, error) {
	u, err := client.Account.
		Create().
		SetBalance(amount).
		Save(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed creating account: %w", err)
	}
	log.Println("account was created: ", u)
	return u, nil
}

func QueryAccount(ctx context.Context, client *ent.Client, owner string) (*ent.Account, error) {
	u, err := client.Account.
		Query().
		Where(account.HasOwnerWith(user.NameEQ(owner))).
		First(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed querying account: %w", err)
	}
	log.Println("account returned: ", u)
	return u, nil
}

func CreateUser(ctx context.Context, client *ent.Client, name string) (*ent.User, error) {
	u, err := client.User.
		Create().
		SetName(name).
		Save(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed creating user: %w", err)
	}
	log.Println("user was created: ", u)
	return u, nil
}
