package xk6_elasticsearch

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/elastic/go-elasticsearch/v8/typedapi/types"
	k6modules "go.k6.io/k6/js/modules"
)

func init() {
	k6modules.Register("k6/x/elasticsearch", new(ElasticSearch))
}

type ElasticSearch struct{}

type Client struct {
	client *elasticsearch.Client
	buf    bytes.Buffer
	res    *esapi.Response
}

type BulkResponse struct {
	Errors bool `json:"errors"`
	Items  []struct {
		Index struct {
			ID     string `json:"_id"`
			Result string `json:"result"`
			Status int    `json:"status"`
			Error  struct {
				Type   string `json:"type"`
				Reason string `json:"reason"`
				Cause  struct {
					Type   string `json:"type"`
					Reason string `json:"reason"`
				} `json:"caused_by"`
			} `json:"error"`
		} `json:"index"`
	} `json:"items"`
}

type SearchResults struct {
	Total int    `json:"total"`
	Hits  []*Hit `json:"hits"`
}

// Hit wraps the document returned in search response.
type Hit struct {
	types.Document
	ID         string        `json:"_id"`
	URL        string        `json:"url"`
	Sort       []interface{} `json:"sort"`
	Highlights *struct {
		Title      []string `json:"title"`
		Alt        []string `json:"alt"`
		Transcript []string `json:"transcript"`
	} `json:"highlights,omitempty"`
}

func (*ElasticSearch) NewClient(connectionStrings []string, username, password string) interface{} {

	config := elasticsearch.Config{Addresses: connectionStrings,
		Username: username,
		Password: password}

	es, i, done := CreateElasticSearchClient(config)
	if done {
		return i
	}

	client := &Client{client: es}

	return client
}

func (*ElasticSearch) NewBasicClient(connectionStrings []string) interface{} {

	config := elasticsearch.Config{Addresses: connectionStrings}

	es, i, done := CreateElasticSearchClient(config)
	if done {
		return i
	}
	client := &Client{client: es}

	return client
}

func CreateElasticSearchClient(config elasticsearch.Config) (*elasticsearch.Client, interface{}, bool) {
	es, err := elasticsearch.NewClient(config)

	if err != nil {
		log.Fatal(err)
		return nil, err, true
	}
	return es, nil, false
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
		log.Printf("[%s] Error indexing document ID=%s", res.Status(), docId)
		return err
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		}
	}

	return nil
}

func (c *Client) AddBatchDocuments(index string, docs map[string]any) error {

	var buf bytes.Buffer
	var raw map[string]interface{}
	var blk *BulkResponse
	for docId, document := range docs {
		// Prepare the metadata payload
		//
		meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s" } }%s`, docId, "\n"))
		// fmt.Printf("%s", meta) // <-- Uncomment to see the payload

		// Prepare the data payload: encode article to JSON
		//
		data, err := json.Marshal(document)
		if err != nil {
			log.Fatalf("Cannot encode article %s: %s", docId, err)
		}

		// Append newline to the data payload
		//
		data = append(data, "\n"...) // <-- Comment out to trigger failure for batch
		// fmt.Printf("%s", data) // <-- Uncomment to see the payload

		// // Uncomment next block to trigger indexing errors -->
		// if a.ID == 11 || a.ID == 101 {
		// 	data = []byte(`{"published" : "INCORRECT"}` + "\n")
		// }
		// // <--------------------------------------------------

		// Append payloads to the buffer (ignoring write errors)
		//
		buf.Grow(len(meta) + len(data))
		buf.Write(meta)
		buf.Write(data)
	}

	res, err := c.client.Bulk(bytes.NewReader(buf.Bytes()), c.client.Bulk.WithIndex(index))
	//log.Printf("object is : " + fmt.Sprintf("%p", &c.vusVars))
	if err != nil {
		log.Fatalf("Failure indexing batch : %s", err)
		return err
	}
	// If the whole request failed, print error and mark all documents as failed
	//
	if res.IsError() {
		if err := json.NewDecoder(res.Body).Decode(&raw); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
			return err
		} else {
			log.Printf("  Error: [%d] %s: %s",
				res.StatusCode,
				raw["error"].(map[string]interface{})["type"],
				raw["error"].(map[string]interface{})["reason"],
			)
			return err
		}
		// A successful response might still contain errors for particular documents...
		//
	} else {
		if err := json.NewDecoder(res.Body).Decode(&blk); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
		} else {
			for _, d := range blk.Items {
				// ... so for any HTTP status above 201 ...
				//
				if d.Index.Status > 201 {

					// ... and print the response status and error information ...
					log.Printf("  Error: [%d]: %s: %s: %s: %s",
						d.Index.Status,
						d.Index.Error.Type,
						d.Index.Error.Reason,
						d.Index.Error.Cause.Type,
						d.Index.Error.Cause.Reason,
					)
				}
			}
		}
	}

	// Close the response body, to prevent reaching the limit for goroutines or file handles
	//
	err = res.Body.Close()
	if err != nil {
		return err
	}

	// Reset the buffer and items counter
	//
	buf.Reset()

	return nil
}

func (c *Client) FindOne(index, docId string) (any, error) {

	var resObj interface{}

	req := esapi.GetRequest{Index: index, DocumentID: docId}

	if res, err := req.Do(context.Background(), c.client); err == nil {
		if err := json.NewDecoder(res.Body).Decode(&resObj); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
			return resObj, err
		}
	} else {
		return resObj, err
	}

	return resObj, nil
}

// Find returns results matching a query, paginated by after.
func (c *Client) Find(index, query string, size int) (any, error) {
	var results SearchResults

	res, err := c.client.Search(
		c.client.Search.WithIndex(index),
		c.client.Search.WithBody(c.constructQuery(query, size)),
	)
	if err != nil {
		return &results, err
	}
	defer res.Body.Close()

	if res.IsError() {
		var e map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&e); err != nil {
			return &results, err
		}
		return &results, fmt.Errorf("[%s] %s: %s", res.Status(), e["error"].(map[string]interface{})["type"], e["error"].(map[string]interface{})["reason"])
	}

	var r any
	if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
		return &results, err
	}

	return r, nil
}
func (c *Client) constructQuery(q string, size int) *strings.Reader {

	// Build a query string from string passed to function
	var query = `{"query": {`

	// Concatenate query string with string passed to method call
	query = query + q

	// Use the strconv.Itoa() method to convert int to string
	query = query + `}, "size": ` + strconv.Itoa(size) + `}`
	//fmt.Println("\nquery:", query)

	// Check for JSON errors
	isValid := json.Valid([]byte(query)) // returns bool

	// Default query is "{}" if JSON is invalid
	if !isValid {
		fmt.Println("constructQuery() ERROR: query string not valid:", query)
		fmt.Println("Using default match_all query")
		query = "{}"
	}

	// Build a new string from JSON query
	var b strings.Builder
	b.WriteString(query)

	// Instantiate a *strings.Reader object from string
	read := strings.NewReader(b.String())

	// Return a *strings.Reader object
	return read
}
func (c *Client) Update(index, docId string, document interface{}) error {

	data, err := json.Marshal(document)
	if err != nil {
		log.Fatalf("Error marshaling document: %s", err)
		return err
	}

	req := esapi.UpdateRequest{
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
		log.Printf("[%s] Error indexing document ID=%s", res, docId)
		return err
	} else {
		// Deserialize the response into a map.
		var r map[string]interface{}
		if err := json.NewDecoder(res.Body).Decode(&r); err != nil {
			log.Printf("Error parsing the response body: %s", err)
		}
	}

	return nil
}
