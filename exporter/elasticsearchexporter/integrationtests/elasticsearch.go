// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package integrationtests

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"

	"github.com/elastic/go-docappender/docappendertest"
	"github.com/elastic/go-elasticsearch/v8"
)

// mockES mocks a few Elasticsearch APIs as required by the
// exporter. It also wraps the elasticsearch client to provide
// a few utility functions for testing.
type mockES struct {
	*elasticsearch.Client
	ServerURL string
	debug     bool

	mu             sync.RWMutex
	countsMap      map[string]int
	mockStatusCode int
}

func newMockESClient(t testing.TB, debug bool) *mockES {
	r := mux.NewRouter()
	r.Use(mux.MiddlewareFunc(func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("X-Elastic-Product", "Elasticsearch")
			next.ServeHTTP(w, r)
		})
	}))
	r.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintln(w, `{"version":{"number":"1.2.3"}}`)
	})

	es := &mockES{
		countsMap:      make(map[string]int),
		mockStatusCode: http.StatusOK,
		debug:          debug,
	}
	countRouter := r.Path("/{index}/_count").Subrouter()
	countRouter.Use(es.mockStatusCodeMiddleware(t))
	countRouter.Handle("", es.countHandler(t))

	bulkRouter := r.Path("/_bulk").Subrouter()
	bulkRouter.Use(es.mockStatusCodeMiddleware(t))
	bulkRouter.Handle("", es.bulkHandler(t))

	esURL := httptest.NewServer(r).URL
	client, err := elasticsearch.NewClient(elasticsearch.Config{
		Addresses: []string{esURL},
	})
	require.NoError(t, err)

	es.ServerURL = esURL
	es.Client = client
	return es
}

// SetReturnStatusCode simulates a failing elasticsearch. Note that this does
// not have any impact on the infor (/) request.
func (m *mockES) SetReturnStatusCode(code int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.mockStatusCode = code
}

func (m *mockES) mockStatusCodeMiddleware(t testing.TB) mux.MiddlewareFunc {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			m.debugLog(t, "status code middleware called with request URI: %s", r.RequestURI)

			m.mu.RLock()
			code := m.mockStatusCode
			m.mu.RUnlock()

			if code/100 == 2 {
				next.ServeHTTP(w, r)
			} else {
				m.debugLog(t, "failing request as orchestrated by the test with status code: %d", code)
				http.Error(w, "orchestrated failure", code)
			}
		})
	}
}

func (m *mockES) countHandler(t testing.TB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		idx := mux.Vars(r)["index"]
		m.debugLog(t, "count handler called with request URI: %s", r.RequestURI)

		m.mu.RLock()
		count := m.countsMap[idx]
		m.mu.RUnlock()

		json.NewEncoder(w).Encode(map[string]int{"count": count})
	}
}

func (m *mockES) bulkHandler(t testing.TB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		m.debugLog(t, "bulk handler called with request URI: %s", r.RequestURI)
		_, response := docappendertest.DecodeBulkRequest(r)

		m.mu.Lock()
		defer m.mu.Unlock()
		for _, itemMap := range response.Items {
			for _, item := range itemMap {
				m.countsMap[item.Index] += 1
			}
		}

		json.NewEncoder(w).Encode(response)
	}
}

func (m *mockES) debugLog(t testing.TB, format string, args ...any) {
	if m.debug {
		t.Logf(format, args...)
	}
}
