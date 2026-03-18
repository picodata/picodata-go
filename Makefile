.PHONY: lint fmt test clean all

all: lint test

lint:
	@which golangci-lint > /dev/null 2>&1 || (echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.3" && exit 1)
	golangci-lint run

fmt:
	go fmt

test:
	go test -v ./...

run-env: stop-env
	docker compose -f build/docker-compose.yml up -d

stop-env:
	docker compose -f build/docker-compose.yml down -v

image:
	docker build -f build/Dockerfile.test-runner -t docker-picodata.binary.picodata.io/picodata-go/go-runner:0.1.1 .

push_image: image
	docker push docker-picodata.binary.picodata.io/picodata-go/go-runner:0.1.1
