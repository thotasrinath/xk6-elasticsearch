package xk6_elasticsearch

import (
	"bytes"
	"encoding/json"
	"fmt"
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
	c.buf.Grow(len(meta) + len(data))
	c.buf.Write(meta)
	c.buf.Write(data)

	c.count++
	c.numItems++

	if c.count > 0 && c.count%c.batch == 0 {
		//fmt.Printf("[%d/%d] ", c.vusVars.currBatch, c.vusVars.numBatches)

		err2, done := BulkIndexDocuments(index, c)
		if done {
			return err2
		}
	}

	return nil
}

func (*ElasticSearch) FlushRemOnBatch(index string) error {

	log.Printf("Teardown invoked")

	clients := GetElasticClients().clients

	for _, c := range clients {
		if c.count > 0 {
			err2, done := BulkIndexDocuments(index, c)
			if done {
				return err2
			}
		}

	}

	return nil
}

func BulkIndexDocuments(index string, c *Client) (error, bool) {
	res, err := c.client.Bulk(bytes.NewReader(c.buf.Bytes()), c.client.Bulk.WithIndex(index))
	c.res = res
	//log.Printf("object is : " + fmt.Sprintf("%p", &c.vusVars))
	if err != nil {
		log.Fatalf("Failure indexing batch %d: %s", c.currBatch, err)
		return err, true
	}
	// If the whole request failed, print error and mark all documents as failed
	//
	if c.res.IsError() {
		c.numErrors += c.numItems
		if err := json.NewDecoder(c.res.Body).Decode(&c.raw); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
			return err, true
		} else {
			log.Printf("  Error: [%d] %s: %s",
				c.res.StatusCode,
				c.raw["error"].(map[string]interface{})["type"],
				c.raw["error"].(map[string]interface{})["reason"],
			)
			return err, true
		}
		// A successful response might still contain errors for particular documents...
		//
	} else {
		if err := json.NewDecoder(c.res.Body).Decode(&c.blk); err != nil {
			log.Fatalf("Failure to to parse response body: %s", err)
		} else {
			for _, d := range c.blk.Items {
				// ... so for any HTTP status above 201 ...
				//
				if d.Index.Status > 201 {
					// ... increment the error counter ...
					//
					c.numErrors++

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
	err1 := c.res.Body.Close()
	if err1 != nil {
		return err1, false
	}

	// Reset the buffer and items counter
	//
	c.buf.Reset()
	c.numItems = 0
	c.count = 0
	c.currBatch++
	return nil, false
}
