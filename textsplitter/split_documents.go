package textsplitter

import (
	"context"
	"errors"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"

	"golang.org/x/sync/errgroup"
	
	"github.com/tmc/langchaingo/schema"
)

// SPLIT_PARALLEL_THREAD can be set as an environment variable to control the number of goroutines to run to split text. Default is 50
const SPLIT_PARALLEL_THREAD = "SPLIT_PARALLEL_THREAD"

// ErrMismatchMetadatasAndText is returned when the number of texts and metadatas
// given to CreateDocuments does not match. The function will not error if the
// length of the metadatas slice is zero.
var ErrMismatchMetadatasAndText = errors.New("number of texts and metadatas does not match")

// SplitDocuments splits documents using a textsplitter.
func SplitDocuments(textSplitter TextSplitter, documents []schema.Document) ([]schema.Document, error) {
	texts := make([]string, 0)
	metadatas := make([]map[string]any, 0)
	for _, document := range documents {
		texts = append(texts, document.PageContent)
		metadatas = append(metadatas, document.Metadata)
	}

	return CreateDocuments(textSplitter, texts, metadatas)
}

// CreateDocuments creates documents from texts and metadatas with a text splitter. If
// the length of the metadatas is zero, the result documents will contain no metadata.
// Otherwise, the numbers of texts and metadatas must match.
func CreateDocuments(textSplitter TextSplitter, texts []string, metadatas []map[string]any) ([]schema.Document, error) {
	if len(metadatas) == 0 {
		metadatas = make([]map[string]any, len(texts))
	}

	if len(texts) != len(metadatas) {
		return nil, ErrMismatchMetadatasAndText
	}

	documents := make([]schema.Document, 0)

	l := sync.Mutex{}
	g, _ := errgroup.WithContext(context.Background())
	g.SetLimit(getFromEnvOrDefault(SPLIT_PARALLEL_THREAD, 50))
	for i := 0; i < len(texts); i++ {
		g.Go(func() error {
			chunks, err := textSplitter.SplitText(texts[i])
			if err != nil {
				return err
			}

			for _, chunk := range chunks {
				// Copy the document metadata
				curMetadata := make(map[string]any, len(metadatas[i]))
				for key, value := range metadatas[i] {
					curMetadata[key] = value
				}

				l.Lock()
				documents = append(documents, schema.Document{
					PageContent: chunk,
					Metadata:    curMetadata,
				})
				l.Unlock()
			}
			return nil
		})

	}

	return documents, g.Wait()
}

func getFromEnvOrDefault(env string, def int) int {
	v, _ := strconv.Atoi(os.Getenv(env))
	if v != 0 {
		return v
	}

	return def
}

// joinDocs comines two documents with the separator used to split them.
func joinDocs(docs []string, separator string) string {
	return strings.TrimSpace(strings.Join(docs, separator))
}

// mergeSplits merges smaller splits into splits that are closer to the chunkSize.
func mergeSplits(splits []string, separator string, chunkSize int, chunkOverlap int, lenFunc func(string) int) []string { //nolint:cyclop
	docs := make([]string, 0)
	currentDoc := make([]string, 0)
	total := 0

	for _, split := range splits {
		totalWithSplit := total + lenFunc(split)
		if len(currentDoc) != 0 {
			totalWithSplit += lenFunc(separator)
		}

		maybePrintWarning(total, chunkSize)
		if totalWithSplit > chunkSize && len(currentDoc) > 0 {
			doc := joinDocs(currentDoc, separator)
			if doc != "" {
				docs = append(docs, doc)
			}

			for shouldPop(chunkOverlap, chunkSize, total, lenFunc(split), lenFunc(separator), len(currentDoc)) {
				total -= lenFunc(currentDoc[0]) //nolint:gosec
				if len(currentDoc) > 1 {
					total -= lenFunc(separator)
				}
				currentDoc = currentDoc[1:] //nolint:gosec
			}
		}

		currentDoc = append(currentDoc, split)
		total += lenFunc(split)
		if len(currentDoc) > 1 {
			total += lenFunc(separator)
		}
	}

	doc := joinDocs(currentDoc, separator)
	if doc != "" {
		docs = append(docs, doc)
	}

	return docs
}

func maybePrintWarning(total, chunkSize int) {
	if total > chunkSize {
		log.Printf(
			"[WARN] created a chunk with size of %v, which is longer then the specified %v\n",
			total,
			chunkSize,
		)
	}
}

// Keep popping if:
//   - the chunk is larger than the chunk overlap
//   - or if there are any chunks and the length is long
func shouldPop(chunkOverlap, chunkSize, total, splitLen, separatorLen, currentDocLen int) bool {
	docsNeededToAddSep := 2
	if currentDocLen < docsNeededToAddSep {
		separatorLen = 0
	}

	return currentDocLen > 0 && (total > chunkOverlap || (total+splitLen+separatorLen > chunkSize && total > 0))
}
