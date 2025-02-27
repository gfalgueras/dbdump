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
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/samber/lo"
)

var CONFIG Config

var SOURCE_ARG string
var TARGET_ARG string
var DB_ARG string
var DB_ARG_RENAME string
var USE_EMPTY_TABLES_ARG bool = true
var ZIPFILENAME_ARG string
var ZIPOUTPUTFOLDER_ARG string = ""

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
		"--default-character-set=utf8mb4",
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

func CheckSourceDatabase(connection Connection, dbName string) error {
	sql, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s:3306)/", connection.User, connection.Password, connection.Ip))

	if err != nil {
		return err
	}

	_, err = sql.Exec(fmt.Sprintf("USE %s", dbName))

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

	err := CheckSourceDatabase(source, sourceDB)
	if err != nil {
		fmt.Print("\r  ┗━ Creating target database ... ✖\n\n")
		return err
	}

	err = CreateTargetDatabase(target, targetDB)
	if err != nil {
		fmt.Print("\r  ┗━ Creating target database ... ✖\n\n")
		return err
	}
	fmt.Print("\r  ┣━ Creating target database ... ✔\n")

	/* Replicate source database onto target database, ignoring some tables */
	fmt.Print("  ┗━ Replicating tables with data ...")
	err = ReplicateTablesWithData(source, target, sourceDB, targetDB)
	if err != nil {
		fmt.Print("\r  ┗━ Replicating tables with data ... ✖\n\n")
		return err
	}
	fmt.Print("\r  ┣━ Replicating tables with data ... ✔\n")

	/* Replicate schema for the ignored tables on the previous step */
	fmt.Print("  ┗━ Replicating tables without data ...")
	err = ReplicateTablesWithoutData(source, target, sourceDB, targetDB)
	if err != nil {
		fmt.Print("\r  ┗━ Replicating tables without data ... ✖\n\n")
		return err
	}
	fmt.Print("\r  ┣━ Replicating tables without data ... ✔\n")

	if USE_EMPTY_TABLES_ARG {
		/* Clear user data */
		fmt.Print("  ┗━ Clear user data ...")
		err = CleanTargetDatabase(target, targetDB)
		if err != nil {
			fmt.Print("\r  ┗━ Clear user data ... ✖\n\n")
			return err
		}
		fmt.Print("\r  ┣━ Clear user data ... ✔\n")
	}

	diff := time.Time{}.Add(time.Since(start)).Format("04:05")
	fmt.Printf("\r  ┗━ Done in %sm\n\n", diff)

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

	var zipFolder string

	if ZIPOUTPUTFOLDER_ARG != "" {
		zipFolder = strings.Trim(ZIPOUTPUTFOLDER_ARG, "/") + "/"
	} else {
		zipFolder = "./"
	}

	err := os.MkdirAll(zipFolder, os.ModePerm)

	if err != nil {
		return err
	}

	zipFileName := fmt.Sprintf("%s_%s.sql", DB_ARG, time.Now().Format("2006_01_02_15_04_05"))
	zipFilePath := zipFolder + zipFileName

	start := time.Now()
	fmt.Printf("Zipping %s from %s to %s ...", DB_ARG, source.Name, zipFolder+ZIPFILENAME_ARG)

	err = CheckSourceDatabase(source, DB_ARG)

	if err != nil {
		fmt.Printf("\rZipping %s from %s to %s ... ✖.\n", DB_ARG, source.Name, zipFolder+ZIPFILENAME_ARG)
		return err
	}

	/* Dump database to sql file */
	dumpcommand := GetDumpCommand(source, DB_ARG, true)

	file, err := os.Create(zipFilePath)

	if err != nil {
		fmt.Printf("\rZipping %s from %s to %s ... ✖.\n", DB_ARG, source.Name, zipFolder+ZIPFILENAME_ARG)
		return err
	}

	dumpcommand.Stdout = file

	err = dumpcommand.Start()

	if err != nil {
		fmt.Printf("\rZipping %s from %s to %s ... ✖.\n\n", DB_ARG, source.Name, zipFolder+ZIPFILENAME_ARG)
		return err
	}

	dumpcommand.Wait()

	file.Close()

	/* Create zip archive */
	archive, err := os.Create(zipFolder + ZIPFILENAME_ARG)

	if err != nil {
		fmt.Printf("\rZipping %s from %s to %s ... ✖.\n\n", DB_ARG, source.Name, zipFolder+ZIPFILENAME_ARG)
		return err
	}

	defer archive.Close()

	zipWriter := zip.NewWriter(archive)

	// Register a custom Deflate compressor.
	zipWriter.RegisterCompressor(zip.Deflate, func(out io.Writer) (io.WriteCloser, error) {
		return flate.NewWriter(out, flate.BestCompression)
	})

	defer zipWriter.Close()

	/* Read sql file */
	fileReader, err := os.Open(zipFilePath)

	if err != nil {
		fmt.Printf("\rZipping %s from %s to %s ... ✖.\n\n", DB_ARG, source.Name, zipFolder+ZIPFILENAME_ARG)
		return err
	}

	/* Copy sql file to zip archive */
	archiveWriter, err := zipWriter.Create(zipFilePath)

	if err != nil {
		fmt.Printf("\rZipping %s from %s to %s ... ✖.\n\n", DB_ARG, source.Name, zipFolder+ZIPFILENAME_ARG)
		return err
	}

	if _, err := io.Copy(archiveWriter, fileReader); err != nil {
		fmt.Printf("\rZipping %s from %s to %s ... ✖.\n\n", DB_ARG, source.Name, zipFolder+ZIPFILENAME_ARG)
		return err
	}

	fileReader.Close()

	if err = os.Remove(zipFilePath); err != nil {
		return err
	}

	diff := time.Time{}.Add(time.Since(start)).Format("04:05")
	fmt.Printf("\rZipping %s from %s to %s ... ✔. Elapsed time: %sm\n\n", DB_ARG, source.Name, zipFolder+ZIPFILENAME_ARG, diff)

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

	var err error

	if DB_ARG_RENAME != "" {
		err = ReplicateDatabase(source, target, DB_ARG, DB_ARG_RENAME)
	} else {
		err = ReplicateDatabase(source, target, DB_ARG, DB_ARG)
	}

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
	fmt.Println("Usage: copy SOURCE TARGET DB DB_RENAME [FLAGS]")
	fmt.Println("       copy SOURCE zip DB [FLAGS]")
	fmt.Println("")
	fmt.Println("Arguments:")
	fmt.Println("  SOURCE    Name of the source database")
	fmt.Println("  TARGET    Name of the target database or zip")
	fmt.Println("  DB        Name of the database to dump")
	fmt.Println("  DB_RENAME New name for the database on the target server")
	fmt.Println("")
	fmt.Println("Flags:")
	fmt.Println("  -h        Show this help")
	fmt.Println("  -i        Performs full dump ignoring post-cleanup queries and empty-tables configuration")
	fmt.Println("  -f        Filename for the generated zip")
	fmt.Println("  -o        Output folder for the zip file")
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

	filenameFlag := false
	outputFolderFlag := false

	for _, arg := range os.Args[4:] {
		if filenameFlag {
			ZIPFILENAME_ARG = arg
			filenameFlag = false
			continue
		} else if outputFolderFlag {
			ZIPOUTPUTFOLDER_ARG = arg
			outputFolderFlag = false
			continue
		}

		if arg == "-i" {
			USE_EMPTY_TABLES_ARG = false
		} else if arg == "-f" || arg == "--file" {
			filenameFlag = true
		} else if arg == "-o" {
			outputFolderFlag = true
		} else if DB_ARG == "" {
			DB_ARG = arg
		} else {
			DB_ARG_RENAME = arg
		}
	}

	ZIPFILENAME_ARG = fmt.Sprintf("%s_%s.zip", DB_ARG, time.Now().Format("2006_01_02_15_04_05"))

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
