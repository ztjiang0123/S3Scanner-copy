package s3scanner

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/sa7mon/s3scanner/bucket"
	"github.com/sa7mon/s3scanner/db"
	log2 "github.com/sa7mon/s3scanner/log"
	"github.com/sa7mon/s3scanner/provider"
	"github.com/sa7mon/s3scanner/worker"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/streadway/amqp"
	"os"
	"reflect"
	"strings"
	"sync"
	"text/tabwriter"
)

type flagSetting struct {
	indentLevel int
	category    int
}

const (
	CategoryInput   int = 0
	CategoryOutput  int = 1
	CategoryOptions int = 2
	CategoryDebug   int = 3
)

var configPaths = []string{".", "/etc/s3scanner/", "$HOME/.s3scanner/"}

var args = ArgCollection{}

func usage() {
	bufferCategoryInput := new(bytes.Buffer)
	bufferCategoryOutput := new(bytes.Buffer)
	bufferCategoryOptions := new(bytes.Buffer)
	bufferCategoryDebug := new(bytes.Buffer)
	categoriesWriters := map[int]*tabwriter.Writer{
		CategoryInput:   tabwriter.NewWriter(bufferCategoryInput, 0, 0, 2, ' ', 0),
		CategoryOutput:  tabwriter.NewWriter(bufferCategoryOutput, 0, 0, 2, ' ', 0),
		CategoryOptions: tabwriter.NewWriter(bufferCategoryOptions, 0, 0, 2, ' ', 0),
		CategoryDebug:   tabwriter.NewWriter(bufferCategoryDebug, 0, 0, 2, ' ', 0),
	}
	flag.VisitAll(func(f *flag.Flag) {
		setting, ok := flagSettings[f.Name]
		if !ok {
			log.Errorf("flag is missing category: %s", f.Name)
			os.Exit(1)
		}
		writer := categoriesWriters[setting.category]

		fmt.Fprintf(writer, "%s  -%s\t", strings.Repeat("   ", setting.indentLevel), f.Name) // Two spaces before -; see next two comments.
		name, usage := flag.UnquoteUsage(f)
		fmt.Fprintf(writer, " %s\t", name)
		fmt.Fprint(writer, usage)
		if !reflect.ValueOf(f.DefValue).IsZero() {
			fmt.Fprintf(writer, " Default: %q", f.DefValue)
		}
		fmt.Fprint(writer, "\n")
	})

	// Output all the categories
	categoriesWriters[CategoryInput].Flush()
	categoriesWriters[CategoryOutput].Flush()
	categoriesWriters[CategoryOptions].Flush()
	categoriesWriters[CategoryDebug].Flush()
	fmt.Fprint(flag.CommandLine.Output(), "INPUT: (1 required)\n", bufferCategoryInput.String())
	fmt.Fprint(flag.CommandLine.Output(), "\nOUTPUT:\n", bufferCategoryOutput.String())
	fmt.Fprint(flag.CommandLine.Output(), "\nOPTIONS:\n", bufferCategoryOptions.String())
	fmt.Fprint(flag.CommandLine.Output(), "\nDEBUG:\n", bufferCategoryDebug.String())

	// Add config file description
	quotedPaths := ""
	for i, b := range configPaths {
		if i != 0 {
			quotedPaths += " "
		}
		quotedPaths += fmt.Sprintf("\"%s\"", b)
	}

	fmt.Fprintf(flag.CommandLine.Output(), "\nIf config file is required these locations will be searched for config.yml: %s\n",
		quotedPaths)
}

var flagSettings = map[string]flagSetting{
	"provider":    {category: CategoryOptions},
	"bucket":      {category: CategoryInput},
	"bucket-file": {category: CategoryInput},
	"mq":          {category: CategoryInput},
	"threads":     {category: CategoryOptions},
	"verbose":     {category: CategoryDebug},
	"version":     {category: CategoryDebug},
	"db":          {category: CategoryOutput},
	"json":        {category: CategoryOutput},
	"enumerate":   {category: CategoryOptions},
}

func Run(version string) {
	// https://twin.sh/articles/39/go-concurrency-goroutines-worker-pools-and-throttling-made-simple
	// https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/aws#AnonymousCredentials

	viper.SetConfigName("config") // name of config file (without extension)
	viper.SetConfigType("yml")    // REQUIRED if the config file does not have the extension in the name
	for _, p := range configPaths {
		viper.AddConfigPath(p)
	}

	flag.StringVar(&args.ProviderFlag, "provider", "aws", fmt.Sprintf(
		"Object storage provider: %s - custom requires config file.",
		strings.Join(provider.AllProviders, ", ")))
	flag.StringVar(&args.BucketName, "bucket", "", "Name of bucket to check.")
	flag.StringVar(&args.BucketFile, "bucket-file", "", "File of bucket names to check.")
	flag.BoolVar(&args.UseMq, "mq", false, "Connect to RabbitMQ to get buckets. Requires config file key \"mq\".")

	flag.BoolVar(&args.WriteToDB, "db", false, "Save results to a Postgres database. Requires config file key \"db.uri\".")
	flag.BoolVar(&args.JSON, "json", false, "Print logs to stdout in JSON format instead of human-readable.")

	flag.BoolVar(&args.DoEnumerate, "enumerate", false, "Enumerate bucket objects (can be time-consuming).")
	flag.IntVar(&args.Threads, "threads", 4, "Number of threads to scan with.")
	flag.BoolVar(&args.Verbose, "verbose", false, "Enable verbose logging.")
	flag.BoolVar(&args.Version, "version", false, "Print version")

	flag.Usage = usage
	flag.Parse()

	if args.Version {
		fmt.Println(version)
		os.Exit(0)
	}

	if argsErr := args.Validate(); argsErr != nil {
		log.Error(argsErr)
		os.Exit(1)
	}

	configureLogging(args)

	if configErr := validateConfig(args); configErr != nil {
		log.Error(configErr)
		os.Exit(1)
	}

	p := buildProvider(args)

	if args.WriteToDB {
		connectDB()
	}

	if args.UseMq {
		runMQ(p, args)
		return
	}
	runLocal(p, args)
	os.Exit(0)
}

// configureLogging sets up the global logger's level, output, and formatter.
func configureLogging(args ArgCollection) {
	log.SetLevel(log.InfoLevel)
	if args.Verbose {
		log.SetLevel(log.DebugLevel)
	}
	log.SetOutput(os.Stdout)
	if args.JSON {
		log.SetFormatter(&log2.NestedJSONFormatter{})
	} else {
		log.SetFormatter(&log.TextFormatter{DisableTimestamp: true})
	}
}

// buildProvider constructs the storage provider selected on the command line,
// exiting the process on any construction error.
func buildProvider(args ArgCollection) provider.StorageProvider {
	var (
		p   provider.StorageProvider
		err error
	)

	if args.ProviderFlag != "custom" {
		p, err = provider.NewProvider(args.ProviderFlag)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
		return p
	}

	// The custom provider is only built when its config section is present;
	// otherwise p is left nil, matching the original behavior.
	if viper.IsSet("providers.custom") {
		log.Debug("found custom provider")
		p, err = provider.NewCustomProvider(
			viper.GetString("providers.custom.address_style"),
			viper.GetBool("providers.custom.insecure"),
			viper.GetStringSlice("providers.custom.regions"),
			viper.GetString("providers.custom.endpoint_format"))
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
	}
	return p
}

// connectDB opens the Postgres connection using the URI from config, exiting on
// failure.
func connectDB() {
	dbConfig := viper.GetString("db.uri")
	log.Debugf("using database URI from config: %s", dbConfig)
	if dbErr := db.Connect(dbConfig, true); dbErr != nil {
		log.Error(dbErr)
		os.Exit(1)
	}
}

// runLocal scans buckets sourced from a file or a single -bucket flag using a
// pool of local workers.
func runLocal(p provider.StorageProvider, args ArgCollection) {
	var wg sync.WaitGroup
	buckets := make(chan bucket.Bucket)

	for i := 0; i < args.Threads; i++ {
		wg.Add(1)
		go worker.Work(&wg, buckets, worker.ScanConfig{
			Provider:    p,
			DoEnumerate: args.DoEnumerate,
			WriteToDB:   args.WriteToDB,
			JSON:        args.JSON,
		})
	}

	if args.BucketFile != "" {
		err := bucket.ReadFromFile(args.BucketFile, buckets)
		close(buckets)
		if err != nil {
			log.Error(err)
			os.Exit(1)
		}
	} else if args.BucketName != "" {
		if !bucket.IsValidS3BucketName(args.BucketName) {
			log.Info(fmt.Sprintf("invalid   | %s", args.BucketName))
			os.Exit(0)
		}
		c := bucket.NewBucket(strings.ToLower(args.BucketName))
		buckets <- c
		close(buckets)
	}

	wg.Wait()
}

// runMQ connects to RabbitMQ and spins off consumer workers that scan buckets
// pulled from the queue.
func runMQ(p provider.StorageProvider, args ArgCollection) {
	var wg sync.WaitGroup
	mqURI := viper.GetString("mq.uri")
	mqName := viper.GetString("mq.queue_name")
	conn, err := amqp.Dial(mqURI)
	if err != nil {
		log.Fatalf("%s: %s", fmt.Sprintf("failed to connect to AMQP URI '%s'", mqURI), err)
	}
	defer conn.Close()

	for i := 0; i < args.Threads; i++ {
		wg.Add(1)
		go worker.WorkMQ(i, &wg, worker.MQConfig{
			ScanConfig: worker.ScanConfig{
				Provider:    p,
				DoEnumerate: args.DoEnumerate,
				WriteToDB:   args.WriteToDB,
			},
			Conn:    conn,
			Queue:   mqName,
			Threads: args.Threads,
		})
	}
	log.Printf("Waiting for messages. To exit press CTRL+C")
	wg.Wait()
}
