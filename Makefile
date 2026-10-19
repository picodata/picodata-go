.PHONY: lint fmt test test-all test-unit test-mutation clean all run-env stop-env wait-env image push_image

COMPOSE = docker compose -f build/docker-compose.yml
RUNNER_IMAGE = docker-picodata.binary.picodata.io/picodata-go/go-runner:0.1.1

all: lint test

lint:
	@which golangci-lint > /dev/null 2>&1 || (echo "golangci-lint not installed. Run: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.11.3" && exit 1)
	golangci-lint run

fmt:
	go fmt

# test runs the whole suite inside the cluster network. Picodata instances
# advertise their container addresses, and the driver connects to the addresses
# the topology reports, so the published ports are of no use here.
test: run-env wait-env
	$(COMPOSE) run --rm test-runner make test-all

# test-all expects a reachable cluster: it is what runs inside the test runner
# and in CI, where the cluster is provided by the job services.
test-all:
	go test -race -v ./...

# test-unit runs the tests that need no cluster, so it works on the host as is.
test-unit:
	go test -race -v -short ./...

# GOFLAGS passes -short to coverage and mutant tests, skipping cluster tests.
test-mutation:
	@which gremlins > /dev/null 2>&1 || (echo "gremlins not installed. Run: go install github.com/go-gremlins/gremlins/cmd/gremlins@v0.6.0" && exit 1)
	GOFLAGS="$(GOFLAGS) -short" gremlins unleash ./topology

run-env: stop-env
	$(COMPOSE) up -d

# stop-env keeps the named volumes: they hold the module and build caches, and
# the cluster state lives in the bind-mounted instance directories instead.
# Those the compose teardown leaves behind, and picodata refuses to restart an
# instance whose tier changed. Recreate the data root through a container and
# make it writable for the non-root user used by current picodata images.
stop-env:
	$(COMPOSE) down
	$(COMPOSE) run --rm -v "$(CURDIR)/tmp:/pico" test-runner bash -c 'rm -rf /pico/data && mkdir -m 0777 /pico/data'

# wait-env blocks until every instance accepts pgproto connections.
wait-env:
	@$(COMPOSE) run --rm test-runner bash -c 'for host in picodata-1-1 picodata-1-2 picodata-2-1 picodata-2-2; do \
		for i in $$(seq 1 60); do \
			(exec 3<>/dev/tcp/$$host/5432) 2>/dev/null && break; \
			if [ $$i = 60 ]; then echo "timeout waiting for $$host"; exit 1; fi; \
			sleep 1; \
		done; \
	done; echo "cluster is up"'

image:
	docker build -f build/Dockerfile.test-runner -t $(RUNNER_IMAGE) .

push_image: image
	docker push $(RUNNER_IMAGE)
