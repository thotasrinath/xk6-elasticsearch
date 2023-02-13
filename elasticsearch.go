package xk6_elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/elastic/go-elasticsearch/v7"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	k6modules "go.k6.io/k6/js/modules"
	"log"
)

func init() {
	k6modules.Register("k6/x/elasticsearch", new(ElasticSearch))
}

type ElasticSearch struct{}

type Client struct {
	client *elasticsearch.Client
}

func (*ElasticSearch) NewClient(connectionStrings []string, username, password string) interface{} {

	config := elasticsearch.Config{Addresses: connectionStrings,
		Username: username,
		Password: password}

	es, err := elasticsearch.NewClient(config)

	if err != nil {
		log.Fatal(err)
		return err
	}
	return &Client{client: es}
}

func (*ElasticSearch) NewBasicClient(connectionStrings []string) interface{} {

	config := elasticsearch.Config{Addresses: connectionStrings}

	es, err := elasticsearch.NewClient(config)

	if err != nil {
		log.Fatal(err)
		return err
	}
	return &Client{client: es}
}

func (c *Client) AddDocument(index, docId string, document interface{}) error {

	data, err := json.Marshal(document)
	if err != nil {
		log.Fatalf("Error marshaling document: %s", err)
		return err
	}

	req := esapi.IndexRequest{
		Index:      index,
		DocumentID: docId,
		Body:       bytes.NewReader(data),
		Refresh:    "true",
	}
	res, err := req.Do(context.Background(), c.client)
	if err != nil {
		log.Fatalf("Failed to index document %s", err)
		return err
	}
	if res.IsError() {
		log.Printf("[%s] Error indexing document ID=%d", res.Status())
		return err
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		} else {
			// Print the response status and indexed document version.
			log.Printf("[%s] %s; version=%d", res.Status(), r["result"], int(r["_version"].(float64)))
		}
	}

	return nil
}
