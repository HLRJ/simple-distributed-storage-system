package utils

import (
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter"
	httpreport "github.com/openzipkin/zipkin-go/reporter/http"
)

const (
	ZIPKIN_HTTP_ENDPOINT = "http://127.0.0.1:9411/api/v2/spans"
)

// NewZipkinTracer creat a zipkin Tracer
func NewZipkinTracer(url, serviceName, hostPort string) (*zipkin.Tracer, reporter.Reporter, error) {

	// init zipkin reporter
	r := httpreport.NewReporter(url)

	//creat a endpoint，used to identify the current service, service name: service address and port
	endpoint, err := zipkin.NewEndpoint(serviceName, hostPort)
	if err != nil {
		return nil, r, err
	}

	// Initialize the tracker to parse span and context
	tracer, err := zipkin.NewTracer(r, zipkin.WithLocalEndpoint(endpoint))
	if err != nil {
		return nil, r, err
	}

	return tracer, r, nil
}
