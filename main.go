package main

import (
	"os"
	"textsql/monetdb"
	"fmt"

	"github.com/alexflint/go-arg"
	"strings"
	"regexp"
	"bufio"
	"io/ioutil"
	"sync"
)

func main() {
	var args struct {
		Interactive    bool     `arg:"-i" help:"load the data and then read commands from the console"`
		Persist        string   `arg:"-p" help:"persist the processed data to the given folder. Allows loading additional data, or running more queries against an already processed data set. This increases initial load time, but may improve successive runs, especially if the amount of additional data loaded is small"`
		SpoolThreshold int      `help:"a number from 0 to 100 that represents the threshold of input size to memory size that causes the tool to spool the input files to disk instead of memory. The default is 50%, meaning if the size of input data is more than 50% of available memory the data will be spooled to a temporary location on disk"`
		Format         string   `arg: "-f" required:"true" help:"specify the format of the data. This is a series of column names and types separated by commas. It looks just like the inside of a create table command in SQL"`
		Match          string   `help:"if used, maps one or more column names to a regular expression. Columns will be parsed out of the line in the order given"`
		Map            string   `help:"if used, indicates how the matched columns from '--match' should be transformed into tabular data. Column names are preceded by $. All other characters are copied literally into the output line"`
		Command        string   `arg:"-c" help:"commands to execute, separated by an ';' or a filename that contains the commands to execute"`
		Inputs         []string `arg:"positional"`
	}

	args.SpoolThreshold = 50
	args.Interactive = false

	arg.MustParse(&args)

	for _, inputPath := range args.Inputs {
		if _, err := os.Stat(inputPath); err != nil {
			fmt.Errorf("Unable to find '%s'\n", inputPath)
		}
	}

	err := monetdb.Startup()
	if err != nil {
		fmt.Errorf("Unable to start database library: %s\n", err)
	}

	conn := monetdb.Connect()
	defer conn.Close()

	err = conn.Execute(fmt.Sprint("CREATE TABLE data(%s)", args.Format))
	if err != nil {
		fmt.Errorf("The format string is incorrect because: %s\n", err)
	}

	if len(args.Map) == 0 {
		// Assume a CSV table and use the highly efficient copy into
		err = conn.Execute(fmt.Sprintf("COPY INTO data FROM (%s)", strings.Join(args.Inputs, ",")))
		if err != nil {
			fmt.Errorf("Unable to load data because: %s\n", err)
		}
	} else {
		re, err := regexp.Compile(args.Match)
		if err != nil {
			fmt.Errorf("Unable to compile the matching expression: %v", err)
			return
		}

		files := make(chan string)
		var wg sync.WaitGroup

		for _, inputPath := range args.Inputs {
			lines := make(chan string)

			wg.Add(2)

			go func() {
				inFile, err := os.Open(inputPath)
				if err != nil {
					fmt.Errorf("Unable to read '%s' because: %v", inputPath, err)
					return
				}
				defer inFile.Close()

				fmt.Printf("processing %s\n", inputPath)

				scanner := bufio.NewScanner(inFile)
				scanner.Split(bufio.ScanLines)

				for scanner.Scan() {
					lines <- scanner.Text()
				}
				close(lines)

				wg.Done()

				fmt.Printf("finished %s\n", inputPath)
			}()

			go func() {
				tmpfile, err := ioutil.TempFile("", "textsql")
				if err != nil {
					fmt.Errorf("Unable to create temporary file because: %v", err)
				}

				fmt.Printf("transforming to %s\n", tmpfile.Name())

				for line := range lines {
					result := []byte{}

					// For each match of the regex in the line. There should be only one.
					for _, submatches := range re.FindAllStringSubmatchIndex(line, -1) {
						result = re.ExpandString(result, args.Map, line, submatches)
					}

					tmpfile.Write(result)
					tmpfile.WriteString("\n")
				}

				tmpfile.Close()
				files <- tmpfile.Name()

				wg.Done()

				fmt.Printf("finished %s\n", tmpfile.Name())
			}()
		}

		go func() {
			wg.Wait()
			close(files)
		}()

		for transformedFilePath := range files {
			fmt.Printf("loading %s\n", transformedFilePath)
			err = conn.Execute(fmt.Sprintf("COPY INTO data FROM (%s)", transformedFilePath))
			if err != nil {
				fmt.Errorf("Unable to load data because: %s\n", err)
			}
			fmt.Printf("loaded %s\n", transformedFilePath)
		}
	}
}
