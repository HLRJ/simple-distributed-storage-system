package utils

import (
	"github.com/openzipkin/zipkin-go"
	"github.com/openzipkin/zipkin-go/reporter"
	httpreport "github.com/openzipkin/zipkin-go/reporter/http"
)

const (
	ZIPKIN_HTTP_ENDPOINT = "http://127.0.0.1:9411/api/v2/spans" //上报到ZipKin中的链路

)

// 创建一个zipkin追踪器
// url:http://localhost:9411/api/v2/spans
// serviceName:服务名，Endpoint标记
// hostPort：ip:port，Endpoint标记
func NewZipkinTracer(url, serviceName, hostPort string) (*zipkin.Tracer, reporter.Reporter, error) {

	// 初始化zipkin reporter
	// reporter可以有很多种，如：logReporter、httpReporter，这里我们只使用httpReporter将span报告给http服务，也就是zipkin的http后台
	r := httpreport.NewReporter(url)

	//创建一个endpoint，用来标识当前服务，服务名：服务地址和端口
	endpoint, err := zipkin.NewEndpoint(serviceName, hostPort)
	if err != nil {
		return nil, r, err
	}

	// 初始化追踪器 主要作用有解析span，解析上下文等
	tracer, err := zipkin.NewTracer(r, zipkin.WithLocalEndpoint(endpoint))
	if err != nil {
		return nil, r, err
	}

	return tracer, r, nil
}
