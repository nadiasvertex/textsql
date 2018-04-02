package main

import (
	"github.com/alexflint/go-arg"
	"os"
	"textsql/monetdb"
	"fmt"
)

func main() {
	var args struct {
		Interactive    bool     `arg:"-i" help:"load the data and then read commands from the console"`
		Persist        string   `arg:"-p" help:"persist the processed data to the given folder. Allows loading additional data, or running more queries against an already processed data set. This increases initial load time, but may improve successive runs, especially if the amount of additional data loaded is small"`
		SpoolThreshold int      `default:"50" help:"a number from 0 to 100 that represents the threshold of input size to memory size that causes the tool to spool the input files to disk instead of memory. The default is 50%, meaning if the size of input data is more than 50% of available memory the data will be spooled to a temporary location on disk"`
		Format         string   `arg: "-f" required:"true" help:"specify the format of the data. This is a series of column names and types separated by commas. It looks just like the inside of a create table command in SQL"`
		Command        string   `arg:"-c" help:"commands to execute, separated by an ';' or a filename that contains the commands to execute"`
		Inputs         []string `arg:"positional"`
	}

	arg.MustParse(&args)

	for _, inputPath := range args.Inputs {
		if _, err := os.Stat(inputPath); err != nil {

		}
	}

	err := monetdb.Startup()
	if err != nil {
		fmt.Errorf("Unable to start database library: %s", err)
	}

	conn := monetdb.Connect()
	defer conn.Close()

	err = conn.Execute(fmt.Sprint("CREATE TABLE data(%s)", args.Format))
	if err != nil {
		fmt.Errorf("The format string is incorrect because: %s", err)
	}
}
