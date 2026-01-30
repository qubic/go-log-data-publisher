package elastic

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"runtime"
	"time"

	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esutil"
)

type Client struct {
	esClient  *elasticsearch.Client
	indexName string
}

func NewClient(esClient *elasticsearch.Client, indexName string) *Client {
	return &Client{
		esClient:  esClient,
		indexName: indexName,
	}
}

type EsDocument struct {
	Id      string
	Payload []byte
}

func (c *Client) BulkIndex(ctx context.Context, documents []*EsDocument) error {
	start := time.Now().UnixMilli()

	bulkIndexer, err := esutil.NewBulkIndexer(esutil.BulkIndexerConfig{
		Index:      c.indexName,
		Client:     c.esClient,
		NumWorkers: min(runtime.NumCPU(), 8),
	})
	if err != nil {
		return fmt.Errorf("creating bulk indexer: %w", err)
	}

	for _, document := range documents {
		item := esutil.BulkIndexerItem{
			Action:       "index",
			DocumentID:   document.Id,
			RequireAlias: true,
			Body:         bytes.NewReader(document.Payload),
			OnFailure: func(ctx context.Context, item esutil.BulkIndexerItem, responseItem esutil.BulkIndexerResponseItem, err error) {
				msg := "Error indexing document"
				if err != nil {
					log.Printf("%s [%s]: %s: [%s]", msg, document.Id, string(document.Payload), err)
				} else {
					log.Printf("%s [%s]: %s: [%s: %s]", msg, document.Id, string(document.Payload), responseItem.Error.Type, responseItem.Error.Reason)
				}
			},
		}
		err = bulkIndexer.Add(ctx, item)
		if err != nil {
			return fmt.Errorf("adding item to bulk indexer: %w", err)
		}
	}

	err = bulkIndexer.Close(ctx)
	if err != nil {
		return fmt.Errorf("closing bulk indexer: %w", err)
	}

	bulkIndexerStats := bulkIndexer.Stats()
	end := time.Now().UnixMilli()

	if bulkIndexerStats.NumFailed > 0 {
		return fmt.Errorf("encountered %d errors while indexing %d documents", bulkIndexerStats.NumFailed, bulkIndexerStats.NumFlushed)
	}

	log.Printf("Indexed %d documents (%d bytes, %d requests) in %d ms.", bulkIndexerStats.NumFlushed, bulkIndexerStats.FlushedBytes, bulkIndexerStats.NumRequests, end-start)
	return nil
}
