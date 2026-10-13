package mockgw_test

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"wetink/internal/classifier"
	"wetink/internal/mockgw"
	"wetink/internal/oai"
)

func TestChatClassifierRelatesByMarkers(t *testing.T) {
	srv := httptest.NewServer(mockgw.Handler())
	defer srv.Close()
	existing := []classifier.Candidate{{ID: 7, Text: "[decision from master-a] Use SQLite."}, {ID: 9, Text: "[decision from master-b] Use Redis for counts."}}
	for _, api := range []string{"responses", "chat"} {
		c, err := classifier.NewChat(oai.New(srv.URL+"/v1", "mock", 5*time.Second), "mock", api, 1)
		if err != nil {
			t.Fatal(err)
		}
		rel, err := c.Relate(context.Background(), "[decision from master-a] Use Postgres. [rel: replaces: SQLite]", existing)
		if err != nil {
			t.Fatal(err)
		}
		if rel.Relation != "replaces" || rel.Target == nil || *rel.Target != 7 {
			t.Fatalf("%s: %+v", api, rel)
		}
		rel, err = c.Relate(context.Background(), "[fact from master-b] Tests pass.", existing)
		if err != nil || rel.Relation != "new" {
			t.Fatalf("%s: %+v %v", api, rel, err)
		}
	}
}
