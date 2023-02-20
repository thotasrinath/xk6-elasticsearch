package xk6_elasticsearch

import (
	"fmt"
	"sync"
)

var lock = &sync.Mutex{}

type ElasticClients struct {
	clients []*Client
}

var elasticClients *ElasticClients

func GetElasticClients() *ElasticClients {
	if elasticClients == nil {
		lock.Lock()
		defer lock.Unlock()
		if elasticClients == nil {
			fmt.Println("Creating ElasticClients instance now.")
			elasticClients = &ElasticClients{}
		} else {
			fmt.Println("ElasticClients instance already created.")
		}
	} else {
		fmt.Println("ElasticClients instance already created.")
	}

	return elasticClients
}
