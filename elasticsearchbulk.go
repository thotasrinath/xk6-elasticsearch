package xk6_elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/elastic/go-elasticsearch/v7/esapi"
	"log"
)

type bulkResponse struct {
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

type VusVars struct {
	buf       bytes.Buffer
	res       *esapi.Response
	err       error
	raw       map[string]interface{}
	blk       *bulkResponse
	numItems  int
	numErrors int
	currBatch int
	count     int
	batch     int
}

func (c *Client) AddDocumentToBatch(index, docId string, document interface{}) error {

	// Prepare the metadata payload
	//
	meta := []byte(fmt.Sprintf(`{ "index" : { "_id" : "%s" } }%s`, docId, "\n"))
	// fmt.Printf("%s", meta) // <-- Uncomment to see the payload

	// Prepare the data payload: encode article to JSON
	//
	data, err := json.Marshal(document)
	if err != nil {
		log.Fatalf("Cannot encode article %d: %s", docId, err)
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
	c.vusVars.buf.Grow(len(meta) + len(data))
	c.vusVars.buf.Write(meta)
	c.vusVars.buf.Write(data)

	c.vusVars.count++
	c.vusVars.numItems++

	if c.vusVars.count > 0 && c.vusVars.count%c.vusVars.batch == 0 {
		//fmt.Printf("[%d/%d] ", c.vusVars.currBatch, c.vusVars.numBatches)

		err2, done := BulkIndexDocuments(index, c)
		if done {
			return err2
		}
	}

	return nil
}

func (c *Client) FlushAndCloseBatch(index string) error {

	log.Printf("Teardown invoked")

	if c.vusVars.count > 0 {
		err2, done := BulkIndexDocuments(index, c)
		if done {
			return err2
		}
	}

	return nil
}

func BulkIndexDocuments(index string, c *Client) (error, bool) {
	res, err := c.client.Bulk(bytes.NewReader(c.vusVars.buf.Bytes()), c.client.Bulk.WithIndex(index))
	c.vusVars.res = res
	//log.Printf("object is : " + fmt.Sprintf("%p", &c.vusVars))
	if err != nil {
		log.Fatalf("Failure indexing batch %d: %s", c.vusVars.currBatch, err)
		return err, true
	}
	// If the whole request failed, print error and mark all documents as failed
	//
	if c.vusVars.res.IsError() {
		c.vusVars.numErrors += c.vusVars.numItems
		if err := json.NewDecoder(c.vusVars.res.Body).Decode(&c.vusVars.raw); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
			return err, true
		} else {
			log.Printf("  Error: [%d] %s: %s",
				c.vusVars.res.StatusCode,
				c.vusVars.raw["error"].(map[string]interface{})["type"],
				c.vusVars.raw["error"].(map[string]interface{})["reason"],
			)
			return err, true
		}
		// A successful response might still contain errors for particular documents...
		//
	} else {
		if err := json.NewDecoder(c.vusVars.res.Body).Decode(&c.vusVars.blk); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
		} else {
			for _, d := range c.vusVars.blk.Items {
				// ... so for any HTTP status above 201 ...
				//
				if d.Index.Status > 201 {
					// ... increment the error counter ...
					//
					c.vusVars.numErrors++

					// ... and print the response status and error information ...
					log.Printf("  Error: [%d]: %s: %s: %s: %s",
						d.Index.Status,
						d.Index.Error.Type,
						d.Index.Error.Reason,
						d.Index.Error.Cause.Type,
						d.Index.Error.Cause.Reason,
					)
				} else {

				}
			}
		}
	}

	// Close the response body, to prevent reaching the limit for goroutines or file handles
	//
	c.vusVars.res.Body.Close()

	// Reset the buffer and items counter
	//
	c.vusVars.buf.Reset()
	c.vusVars.numItems = 0
	c.vusVars.count = 0
	c.vusVars.currBatch++
	return nil, false
}
