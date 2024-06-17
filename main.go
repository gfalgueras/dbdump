package main

import (
	"archive/zip"
	"compress/flate"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"slices"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/samber/lo"
)

var CONFIG Config

var SOURCE_ARG string
var TARGET_ARG string
var DB_ARG string
var USE_EMPTY_TABLES_ARG bool = true
var ZIPFILENAME_ARG string

type Config struct {
	Servers              []Connection
	Empty_tables         []string
	Transactions         [][]string
	Post_process_queries []string
}

type Connection struct {
	Name     string
	Ip       string
	User     string
	Password string
}

func GetDumpCommand(connection Connection, dbName string, withData bool) *exec.Cmd {
	args := []string{
		fmt.Sprintf("--host=%s", connection.Ip),
		fmt.Sprintf("--user=%s", connection.User),
		"--skip-lock-tables",
		"--max-allowed-packet=2GB",
		"--single-transaction",
		"--set-gtid-purged=OFF",
	}

	if connection.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", connection.Password))
	}

	args = append(args, dbName)

	if USE_EMPTY_TABLES_ARG && len(CONFIG.Empty_tables) > 0 {
		if withData {
			tables := lo.Map(CONFIG.Empty_tables, func(table string, index int) string {
				return fmt.Sprintf("--ignore-table=%s.%s", dbName, table)
			})

			args = append(args, tables...)
		} else {
			args = append(args, "--no-data", "--no-create-db", "--no-tablespaces", "--tables")
			args = append(args, CONFIG.Empty_tables...)
		}
	}

	return exec.Command("mysqldump", args...)
}

func GetMysqlCommand(connection Connection, dbName string) *exec.Cmd {
	args := []string{
		fmt.Sprintf("--host=%s", connection.Ip),
		fmt.Sprintf("--user=%s", connection.User),
		fmt.Sprintf("--database=%s", dbName),
		"--max-allowed-packet=2GB",
		"--ssl-mode=DISABLED",
	}

	if connection.Password != "" {
		args = append(args, fmt.Sprintf("--password=%s", connection.Password))
	}

	args = append(args, dbName)

	return exec.Command("mysql", args...)
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

func CreateTargetDatabase(connection Connection, dbName string) error {
	sql, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/", connection.User, connection.Password, connection.Ip))

	if err != nil {
		return err
	}

	_, err = sql.Exec(fmt.Sprintf("DROP DATABASE IF EXISTS %s", dbName))

	if err != nil {
		return err
	}

	_, err = sql.Exec(fmt.Sprintf("CREATE DATABASE %s", dbName))

	if err != nil {
		return err
	}

	return nil
}

func ReplicateTablesWithData(source Connection, target Connection, sourceDB string, targetDB string) error {
	c1 := GetDumpCommand(source, sourceDB, true)
	c2 := GetMysqlCommand(target, targetDB)

	err := PipeCommands(c1, c2)

	if err != nil {
		return err
	}

	return nil
}

func ReplicateTablesWithoutData(source Connection, target Connection, sourceDB string, targetDB string) error {
	c1 := GetDumpCommand(source, sourceDB, false)
	c2 := GetMysqlCommand(target, targetDB)

	err := PipeCommands(c1, c2)

	if err != nil {
		return err
	}

	return nil
}

func CleanTargetDatabase(connection Connection, target string) error {
	sql, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/", connection.User, connection.Password, connection.Ip))

	if err != nil {
		return err
	}

	_, err = sql.Exec(fmt.Sprintf("USE %s", target))

	if err != nil {
		return err
	}

	for _, query := range CONFIG.Post_process_queries {
		_, err = sql.Exec(query)

		if err != nil {
			return err
		}
	}

	return nil
}

func ReplicateDatabase(source Connection, target Connection, sourceDB string, targetDB string) error {
	fmt.Printf("  %s:%s ━━━▶ %s:%s\n", source.Name, sourceDB, target.Name, targetDB)

	start := time.Now()

	/* Replicate source database onto target database, ignoring some tables */
	fmt.Print("  ┗━ Creating target database ...")
	err := CreateTargetDatabase(target, targetDB)
	if err != nil {
		fmt.Print("\r  ┗━ Creating target database ... ✖\n")
		return err
	}
	fmt.Print("\r  ┣━ Creating target database ... ✔\n")

	/* Replicate source database onto target database, ignoring some tables */
	fmt.Print("  ┗━ Replicating tables with data ...")
	err = ReplicateTablesWithData(source, target, sourceDB, targetDB)
	if err != nil {
		fmt.Print("\r  ┗━ Replicating tables with data ... ✖\n")
		return err
	}
	fmt.Print("\r  ┣━ Replicating tables with data ... ✔\n")

	/* Replicate schema for the ignored tables on the previous step */
	fmt.Print("  ┗━ Replicating tables without data ...")
	err = ReplicateTablesWithoutData(source, target, sourceDB, targetDB)
	if err != nil {
		fmt.Print("\r  ┗━ Replicating tables without data ... ✖\n")
		return err
	}
	fmt.Print("\r  ┣━ Replicating tables without data ... ✔\n")

	if USE_EMPTY_TABLES_ARG {
		/* Clear user data */
		fmt.Print("  ┗━ Clear user data ...")
		err = CleanTargetDatabase(target, targetDB)
		if err != nil {
			fmt.Print("\r  ┗━ Clear user data ... ✖\n")
			return err
		}
		fmt.Print("\r  ┣━ Clear user data ... ✔\n")
	}

	diff := time.Time{}.Add(time.Since(start)).Format("04:05")
	fmt.Printf("\r  ┗━ Done in %sm\n", diff)

	return nil
}

func RunBulk() error {
	sourceIndex := slices.IndexFunc(CONFIG.Servers, func(c Connection) bool {
		return c.Name == SOURCE_ARG
	})

	if sourceIndex == -1 {
		return fmt.Errorf("source '%s' not found in config file", SOURCE_ARG)
	}

	source := CONFIG.Servers[sourceIndex]

	targetIndex := slices.IndexFunc(CONFIG.Servers, func(c Connection) bool {
		return c.Name == TARGET_ARG
	})

	if sourceIndex == -1 {
		return fmt.Errorf("source '%s' not found in config file", SOURCE_ARG)
	}

	target := CONFIG.Servers[targetIndex]

	start := time.Now()

	fmt.Println("\nStart bulk dump")

	counter := 0

	for _, transaction := range CONFIG.Transactions {
		err := ReplicateDatabase(source, target, transaction[0], transaction[1])

		if err != nil {
			fmt.Println(err.Error())
			break
		}

		counter++
	}

	diff := time.Time{}.Add(time.Since(start)).Format("04:05")
	fmt.Printf("%d databases done in %sm\n", counter, diff)

	return nil
}

func CopyToZip() error {
	USE_EMPTY_TABLES_ARG = false

	sourceIndex := slices.IndexFunc(CONFIG.Servers, func(c Connection) bool {
		return c.Name == SOURCE_ARG
	})

	if sourceIndex == -1 {
		return fmt.Errorf("source '%s' not found in config file", SOURCE_ARG)
	}

	source := CONFIG.Servers[sourceIndex]

	start := time.Now()
	fmt.Printf("Zipping %s ...", DB_ARG)

	/* Dump database to sql file */
	dumpcommand := GetDumpCommand(source, DB_ARG, true)

	zipFileName := fmt.Sprintf("%s_%s.sql", DB_ARG, time.Now().Format("2006_01_02_15_04_05"))
	file, err := os.Create(zipFileName)

	if err != nil {
		fmt.Printf("\rZipping %s ... ✖.\n", DB_ARG)
		return err
	}

	defer file.Close()

	dumpcommand.Stdout = file

	err = dumpcommand.Start()

	if err != nil {
		fmt.Printf("\rZipping %s ... ✖.\n", DB_ARG)
		return err
	}

	dumpcommand.Wait()

	/* Create zip archive */
	archive, err := os.Create(fmt.Sprintf("./%s", ZIPFILENAME_ARG))

	if err != nil {
		fmt.Printf("\rZipping %s ... ✖.\n", DB_ARG)
		return err
	}

	defer archive.Close()

	zipWriter := zip.NewWriter(archive)

	// Register a custom Deflate compressor.
	zipWriter.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(out, flate.BestCompression)
	})

	/* Read sql file */
	fileReader, err := os.Open(zipFileName)

	if err != nil {
		fmt.Printf("\rZipping %s ... ✖.\n", DB_ARG)
		return err
	}

	defer fileReader.Close()

	/* Copy sql file to zip archive */
	archiveWriter, err := zipWriter.Create(zipFileName)

	if err != nil {
		fmt.Printf("\rZipping %s ... ✖.\n", DB_ARG)
		return err
	}

	if _, err := io.Copy(archiveWriter, fileReader); err != nil {
		fmt.Printf("\rZipping %s ... ✖.\n", DB_ARG)
		return err
	}

	zipWriter.Close()

	os.Remove(zipFileName)

	diff := time.Time{}.Add(time.Since(start)).Format("04:05")
	fmt.Printf("\rZipping %s ... ✔. Elapsed time: %sm\n", DB_ARG, diff)

	return nil
}

func CopyToDb() error {
	sourceIndex := slices.IndexFunc(CONFIG.Servers, func(c Connection) bool {
		return c.Name == SOURCE_ARG
	})

	if sourceIndex == -1 {
		return fmt.Errorf("source '%s' not found in config file", SOURCE_ARG)
	}

	source := CONFIG.Servers[sourceIndex]

	targetIndex := slices.IndexFunc(CONFIG.Servers, func(c Connection) bool {
		return c.Name == TARGET_ARG
	})

	if targetIndex == -1 {
		return fmt.Errorf("source '%s' not found in config file", SOURCE_ARG)
	}

	target := CONFIG.Servers[targetIndex]

	err := ReplicateDatabase(source, target, DB_ARG, DB_ARG)

	if err != nil {
		return err
	}

	return nil
}

func RunCopy() error {
	if TARGET_ARG == "zip" {
		return CopyToZip()
	} else {
		return CopyToDb()
	}
}

func HelpDump() {
	fmt.Println("Usage: dump [COMMAND] [POSITIONAL ARGS] [FLAGS]")
	fmt.Println("")
	fmt.Println("Commands: bulk, copy")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("  -h       Show help for the command")
}

func HelpCopy() {
	fmt.Println("Usage: copy SOURCE TARGET DB [FLAGS]")
	fmt.Println("       copy SOURCE zip DB [FLAGS]")
	fmt.Println("")
	fmt.Println("Arguments:")
	fmt.Println("  SOURCE   Name of the source database")
	fmt.Println("  TARGET   Name of the target database or zip")
	fmt.Println("  DB       Name of the database to dump")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("  -h       Show this help")
	fmt.Println("  -i       Performs full dump ignoring post-cleanup queries and empty-tables configuration")
	fmt.Println("  -f       Filename for the generated zip")
}

func HelpBulk() {
	fmt.Println("Usage: bulk SOURCE TARGET [FLAGS]")
	fmt.Println("")
	fmt.Println("Arguments:")
	fmt.Println("  SOURCE   Name of the source database")
	fmt.Println("  TARGET   Name of the target database")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("  -h       Show this help")
	fmt.Println("  -i       Performs full dump ignoring post-cleanup queries and empty-tables configuration")
}

func main() {
	if len(os.Args) < 2 || (os.Args[1] != "bulk" && os.Args[1] != "copy") {
		HelpDump()
		return
	}

	command := os.Args[1]

	if len(os.Args) == 3 && (os.Args[1] == "--help" || os.Args[1] == "-h") {
		if command == "copy" {
			HelpCopy()
			return
		} else if command == "bulk" {
			HelpBulk()
			return
		} else {
			HelpDump()
			return
		}
	}

	file := "config.json"

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

	if len(os.Args) < 4 {
		HelpDump()
		return
	}

	SOURCE_ARG = os.Args[2]
	TARGET_ARG = os.Args[3]

	if len(os.Args) == 5 {
		DB_ARG = os.Args[4]
	}

	ZIPFILENAME_ARG = fmt.Sprintf("%s_%s.zip", DB_ARG, time.Now().Format("2006_01_02_15_04_05"))

	fileFlag := false

	for _, arg := range os.Args[4:] {
		if fileFlag {
			ZIPFILENAME_ARG = arg
			break
		}

		if arg == "-i" {
			USE_EMPTY_TABLES_ARG = false
		} else if arg == "-f" || arg == "--file" {
			fileFlag = true
		}
	}

	if command == "bulk" {
		err = RunBulk()

		if err != nil {
			fmt.Println(err)
		}
	} else if command == "copy" {
		err = RunCopy()

		if err != nil {
			fmt.Println(err)
		}
	}
}
