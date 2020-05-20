package main

import (
	"bytes"
	"flag"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/pkg/errors"
	"io/ioutil"
	"log"
	"path/filepath"
	"strings"
)

const ExtJson = ".json"

func main() {

	elasticUrl := flag.String("url", "http://localhost:9200", "elastic url")
	path := flag.String("path", "./", "path to templates")

	flag.Parse()

	var (
		es  *elasticsearch.Client
		err error
	)
	cfg := elasticsearch.Config{
		Addresses: []string{
			*elasticUrl,
		},
		// ...
	}
	es, err = elasticsearch.NewClient(cfg)

	if err != nil {
		log.Fatalf("Error creating the client: %s", err)
	}

	println(*path, es)

	files, err := ioutil.ReadDir(*path)
	if err != nil {
		log.Fatal(err)
	}

	templates := map[string][]byte{}

	for _, f := range files {
		ext := filepath.Ext(f.Name())

		if ext != ExtJson {
			continue
		}
		templateName := strings.Replace(f.Name(), ".json", "", 1)
		parsePath := filepath.FromSlash(fmt.Sprintf("%s/%s", *path, f.Name()))
		fileContent, err := ioutil.ReadFile(parsePath)
		if err != nil {
			log.Fatal(errors.Wrap(err, "Error while getting file"))
		}
		templates[templateName] = fileContent
	}

	runMigration(es, templates)
}

func runMigration(es *elasticsearch.Client, templates map[string][]byte) {
	for name, template := range templates {
		log.Printf("elastic.run_migration.template.%s", name)
		response, err := es.Indices.PutTemplate(name, bytes.NewReader(template))
		if err != nil {
			log.Println(errors.Wrap(err, "Put template error"))
			log.Println(err)
		}
		if response.StatusCode != 200 {
			log.Println(errors.New("Put template error - 1 "))
			log.Println(response)
		}
	}
}
