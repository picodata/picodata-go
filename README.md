# picodata-go - Picodata driver based on pgx

picodata-go is a pure Golang driver for [Picodata](https://git.picodata.io/core/picodata).  
With power of [pgx/pgxpool](https://github.com/jackc/pgx) library, peer discovery and embedded load balancing
it provides seamless experience of using distributed databases.  

## Key features:
1. Instance discovery
2. Load balancing with public API
3. Automatic topology managing

## Example of usage

```go
package main

import (
	"context"
	"fmt"
	"os"

	picogo "git.picodata.io/core/picodata-go"
	"git.picodata.io/core/picodata-go/strats"
)

func main() {
	// urlExample := "postgres://username:password@localhost:5432"
	pool, err := picogo.New(context.Background(), os.Getenv("PICODATA_URL"), picogo.WithBalancerStrat(strats.RandomStrat))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connect to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()
	// CREATE TABLE items (id INTEGER NOT NULL,name TEXT NOT NULL,stock INTEGER,PRIMARY KEY (id)) USING memtx DISTRIBUTED BY (id) OPTION (TIMEOUT = 3.0);
	// INSERT INTO items VALUES
	// (1, 'bricks', 1123),
	// (2, 'panels', 998),
	// (3, 'piles', 177);
	var (
		id    int
		name  string
		stock int
	)
	err = pool.QueryRow(context.Background(), "select * from items where id=$1::integer", 2).Scan(&id, &name, &stock)
	if err != nil {
		fmt.Fprintf(os.Stderr, "QueryRow failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Println(id, name, stock)
}
```

picodata-go Pool class follows and provied the same API as pgx/v5/pgxpool. Also, Pool's New() and NewWithConfig() methods receive optional arguments - functional options,
that can modify default settings. You can check them out in [pool_opts.go](./pool_opts.go) file:

```go
func WithBalancerFunc(fn s.BalanceStratFunc) PoolOption {
...
}

func WithBalancerStrat(strategy s.BalanceStratType) PoolOption {
...
}
```
