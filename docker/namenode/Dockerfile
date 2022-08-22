FROM golang:alpine

ENV CGO_ENABLED 0
ENV GOOS linux
ENV GOPROXY https://goproxy.cn,direct

WORKDIR /build
COPY go.mod .
COPY go.sum .
RUN go mod download
COPY . .
RUN go build -o bin/namenode cmd/namenode/main.go

ENTRYPOINT ["/build/bin/namenode"]