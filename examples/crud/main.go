package main

import (
	"bufio"
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"strconv"
	"strings"

	picogo "git.picodata.io/core/picodata-go"
	"git.picodata.io/core/picodata-go/strats"
)

var pool *picogo.Pool

func main() {
	var err error

	url := os.Getenv("PICODATA_CONNECTION_URL")
	if url == "" {
		fmt.Fprint(os.Stderr, "PICODATA_CONNECTION_URL is not set\n")
		os.Exit(1)
	}

	pool, err = picogo.New(context.Background(), url, picogo.WithBalancerStrat(strats.RandomStrat))
	if err != nil {
		fmt.Fprintf(os.Stderr, "Unable to connection to database: %v\n", err)
		os.Exit(1)
	}
	defer pool.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("(db) > ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error while reading command: %v\n", err)
			continue
		}

		line = strings.TrimSpace(line)

		args := strings.Split(line, " ")

		switch args[0] {
		case "help":
			printHelp()
			continue
		case "list":
			err = listTasks()
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable to list tasks: %v\n", err)
				continue
			}
		case "add":
			err = addTask(strings.Join(args[1:], " "))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable to add task: %v\n", err)
				continue
			}
		case "update":
			n, err := strconv.ParseInt(args[1], 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable convert task_num into int32: %v\n", err)
				continue
			}
			err = updateTask(int32(n), strings.Join(args[2:], " "))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable to update task: %v\n", err)
				continue
			}
		case "remove":
			n, err := strconv.ParseInt(args[1], 10, 32)
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable convert task_num into int32: %v\n", err)
				continue
			}
			err = removeTask(int32(n))
			if err != nil {
				fmt.Fprintf(os.Stderr, "Unable to remove task: %v\n", err)
				continue
			}
		case "exit":
			return
		default:
			fmt.Fprintln(os.Stderr, "Invalid command")
			printHelp()
			continue
		}
	}

}

func listTasks() error {
	rows, _ := pool.Query(context.Background(), "select * from tasks")

	for rows.Next() {
		var id int32
		var description string
		err := rows.Scan(&id, &description)
		if err != nil {
			return err
		}
		fmt.Printf("%d -> %s\n", id, description)
	}

	return rows.Err()
}

func addTask(description string) error {
	id := rand.Int32()
	_, err := pool.Exec(context.Background(), "insert into tasks values($1::integer, $2::string)", id, description)
	return err
}

func updateTask(itemNum int32, description string) error {
	_, err := pool.Exec(context.Background(), "update tasks set description=$1::string where id=$2::integer", description, itemNum)
	return err
}

func removeTask(itemNum int32) error {
	_, err := pool.Exec(context.Background(), "delete from tasks where id=$1::integer", itemNum)
	return err
}

func printHelp() {
	fmt.Print(`
Commands:

  help
  list
  add string
  update id string
  remove id
  exit

Example:

  add 'pico data'
  list
`)
}
