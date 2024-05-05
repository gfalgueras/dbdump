package main

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/samber/lo"
)

var CONFIG Config

type Config struct {
	User_source          string
	User_target          string
	Ip_source            string
	Ip_target            string
	Passw_source         string
	Passw_target         string
	Tables_skip_data     []string
	Transactions         [][]string
	Post_process_queries []string
}

func BuildDumpArgs(db string, ip string, user string, passw string) []string {
	args := []string{
		fmt.Sprintf("--host=%s", ip),
		fmt.Sprintf("--user=%s", user),
		"--skip-lock-tables",
		"--max-allowed-packet=2GB",
		"--single-transaction",
		"--set-gtid-purged=OFF",
	}

	if passw != "" {
		args = append(args, fmt.Sprintf("--password=%s", passw))
	}

	args = append(args, db)

	return args
}

func BuildMysqlArgs(db string, ip string, user string, passw string) []string {
	args := []string{
		fmt.Sprintf("--host=%s", ip),
		fmt.Sprintf("--user=%s", user),
		fmt.Sprintf("--database=%s", db),
		"--max-allowed-packet=2GB",
		"--ssl-mode=DISABLED",
	}

	if passw != "" {
		args = append(args, fmt.Sprintf("--password=%s", passw))
	}

	return args
}

func PipeCommands(c1 *exec.Cmd, c2 *exec.Cmd) error {
	pr, pw := io.Pipe()

	c1.Stdout = pw
	c2.Stdin = pr
	c2.Stdout = os.Stdout

	err := c1.Start()

	if err != nil {
		return err
	}

	err = c2.Start()

	if err != nil {
		return err
	}

	go func() {
		defer pw.Close()

		c1.Wait()
	}()

	err = c2.Wait()

	if err != nil {
		return err
	}

	return nil
}

func CreateTargetDatabase(db *sql.DB, target string) error {
	_, err := db.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", target))

	if err != nil {
		return err
	}

	_, err = db.Exec(fmt.Sprintf("CREATE DATABASE %s", target))

	if err != nil {
		return err
	}

	return nil
}

func ReplicateTablesWithData(db *sql.DB, source string, target string) error {
	dumpArgs := BuildDumpArgs(source, CONFIG.Ip_source, CONFIG.User_source, CONFIG.Passw_source)

	tables := lo.Map(CONFIG.Tables_skip_data, func(table string, index int) string {
		return fmt.Sprintf("--ignore-table=%s.%s", source, table)
	})

	dumpArgs = append(dumpArgs, tables...)

	c1 := exec.Command("mysqldump", dumpArgs...)

	mysqlArgs := BuildMysqlArgs(target, CONFIG.Ip_target, CONFIG.User_target, CONFIG.Passw_target)

	c2 := exec.Command("mysql", mysqlArgs...)

	err := PipeCommands(c1, c2)

	if err != nil {
		return err
	}

	return nil
}

func ReplicateTablesWithoutData(db *sql.DB, source string, target string) error {
	dumpArgs := BuildDumpArgs(source, CONFIG.Ip_source, CONFIG.User_source, CONFIG.Passw_source)

	dumpArgs = append(dumpArgs, "--no-data", "--no-create-db", "--no-tablespaces", "--tables")
	dumpArgs = append(dumpArgs, CONFIG.Tables_skip_data...)

	c1 := exec.Command("mysqldump", dumpArgs...)

	mysqlArgs := BuildMysqlArgs(target, CONFIG.Ip_target, CONFIG.User_target, CONFIG.Passw_target)

	c2 := exec.Command("mysql", mysqlArgs...)

	err := PipeCommands(c1, c2)

	if err != nil {
		return err
	}

	return nil
}

func CleanTargetDatabase(db *sql.DB, target string) error {
	_, err := db.Exec(fmt.Sprintf("USE %s", target))

	if err != nil {
		return err
	}

	for _, query := range CONFIG.Post_process_queries {
		_, err = db.Exec(query)

		if err != nil {
			return err
		}
	}

	return nil
}

func ReplicateDatabase(db *sql.DB, source string, target string) error {
	fmt.Printf("  %s ━━━▶ %s:\n", source, target)

	/* Replicate source database onto target database, ignoring some tables */
	fmt.Print("  ┗━ Creating target database ...")
	err := CreateTargetDatabase(db, target)
	if err != nil {
		fmt.Print("\r  ┗━ Creating target database ... ✖\n")
		return err
	}
	fmt.Print("\r  ┣━ Creating target database ... ✔\n")

	/* Replicate source database onto target database, ignoring some tables */
	fmt.Print("  ┗━ Replicating tables with data ...")
	err = ReplicateTablesWithData(db, source, target)
	if err != nil {
		fmt.Print("\r  ┗━ Replicating tables with data ... ✖\n")
		return err
	}
	fmt.Print("\r  ┣━ Replicating tables with data ... ✔\n")

	/* Replicate schema for the ignored tables on the previous step */
	fmt.Print("  ┗━ Replicating tables without data ...")
	err = ReplicateTablesWithoutData(db, source, target)
	if err != nil {
		fmt.Print("\r  ┗━ Replicating tables without data ... ✖\n")
		return err
	}
	fmt.Print("\r  ┣━ Replicating tables without data ... ✔\n")

	/* Clear user data */
	fmt.Print("  ┗━ Clear user data ...")
	err = CleanTargetDatabase(db, target)
	if err != nil {
		fmt.Print("\r  ┗━ Clear user data ... ✖\n")
		return err
	}
	fmt.Print("\r  ┗━ Clear user data ... ✔\n\n")

	return nil
}

func main() {
	file := "config.json"

	if len(os.Args) == 2 {
		file = os.Args[1]
	}

	data, err := os.ReadFile(file)

	if err != nil {
		fmt.Print(err)
		return
	}

	err = json.Unmarshal(data, &CONFIG)

	if err != nil {
		fmt.Print(err)
		return
	}

	if CONFIG.Ip_source == "" || CONFIG.Ip_target == "" || CONFIG.User_source == "" || CONFIG.User_target == "" {
		fmt.Print("Missing configuration params")
		return
	}

	db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/", CONFIG.User_target, CONFIG.Passw_target, CONFIG.Ip_target))

	if err != nil {
		fmt.Print(err.Error())
		return
	}

	start := time.Now()

	fmt.Println("\nStart test dump")

	counter := 0

	for _, transaction := range CONFIG.Transactions {
		err := ReplicateDatabase(db, transaction[0], transaction[1])

		if err != nil {
			fmt.Println(err.Error())
			break
		}

		counter++
	}

	diff := time.Time{}.Add(time.Since(start)).Format("04:05")
	fmt.Printf("\nDatabases: %d. Elapsed time: %sm\n", counter, diff)
}
