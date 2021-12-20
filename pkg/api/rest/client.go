package rest

import (
	"net/url"
	"strings"

	"github.com/pingcap/ticdc/pkg/httputil"
)

// HTTPInterface includes a set of operations to interact with cdc REST apis
type RESTInterface interface {
	Verb(verb string) *Request
	Post() *Request
	Put() *Request
	Get() *Request
	Delete() *Request
}

type RESTClient struct {
	// base is the root URL for all invocations of the client.
	base *url.URL

	// versionedAPIPath is a http url prefix with api version. eg. /api/v1.
	versionedAPIPath string

	// Client is a wrapped http client.
	Client *httputil.Client
}

// NewRESTClient creates a new RESTClient.
func NewRESTClient(baseURL *url.URL, versionedAPIPath string, client *httputil.Client) (*RESTClient, error) {
	base := *baseURL
	if !strings.HasSuffix(base.Path, "/") {
		base.Path += "/"
	}
	base.RawQuery = ""
	base.Fragment = ""

	return &RESTClient{
		base:             &base,
		versionedAPIPath: versionedAPIPath,
		Client:           client,
	}, nil
}

// Verb begins a request with a verb (GET, POST, PUT, DELETE).
func (c *RESTClient) Verb(verb string) *Request {
	return NewRequest(c).Verb(verb)
}

// Post begins a POST request. Short for c.Verb("POST").
func (c *RESTClient) Post() *Request {
	return c.Verb("POST")
}

// Put begins a PUT request. Short for c.Verb("PUT").
func (c *RESTClient) Put() *Request {
	return c.Verb("PUT")
}

// Delete begins a DELETE request. Short for c.Verb("DELETE").
func (c *RESTClient) Delete() *Request {
	return c.Verb("DELETE")
}

// Get begins a GET request. Short for c.Verb("GET").
func (c *RESTClient) Get() *Request {
	return c.Verb("GET")
}
