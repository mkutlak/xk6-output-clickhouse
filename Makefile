.PHONY: build clean test test-unit test-coverage fmt lint vet modernize tidy check update install-tools docker-build docker-clean docker-compose-up docker-compose-down docker-compose-logs docker-dev docker-test docker-clean-all help all

# Project variables
REPO_OWNER ?= mkutlak
REPO_NAME ?= xk6-output-clickhouse
EXTENSION_MODULE ?= github.com/$(REPO_OWNER)/$(REPO_NAME)
XK6_VERSION ?= $(shell cat .xk6-version 2>/dev/null || echo latest)
export XK6_VERSION
GOLANGCI_LINT_VERSION ?= v2.14.0

# CI/CD variables
IMAGE_NAME ?= ghcr.io/$(REPO_OWNER)/$(REPO_NAME)
VERSION ?= latest

# Default target
all: check build

# Build the k6 binary with the extension
build:
	@echo "Building k6 with $(REPO_NAME)..."
	@mkdir -p bin/
	@go run go.k6.io/xk6/cmd/xk6@$(XK6_VERSION) build --output bin/k6 --with $(EXTENSION_MODULE)=.
	@echo "Build complete: ./bin/k6"

# Run all tests, including integration tests that require Docker/testcontainers
test:
	@echo "Running tests..."
	@go test -v -race ./...

# Run unit tests only (short mode; skips Docker/testcontainers integration tests)
test-unit:
	@echo "Running unit tests (short mode, no Docker required)..."
	@go test -short -race ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@mkdir -p tests/
	@go test -v -race -coverprofile=tests/coverage.out -covermode=atomic ./...
	@go tool cover -html=tests/coverage.out -o tests/coverage.html
	@echo "Coverage report generated: tests/coverage.html"

# Format code
fmt:
	@echo "Formatting code..."
	@go fmt ./...

# Run linter (requires golangci-lint)
lint:
	@echo "Running linter..."
	@which golangci-lint > /dev/null || (echo "golangci-lint not found. Run 'make install-tools' to install it." && exit 1)
	@golangci-lint run ./...

# Run go vet
vet:
	@echo "Running go vet..."
	@go vet ./...

# Apply Go modernizers (go fix)
modernize:
	@echo "Applying Go modernizers..."
	@go fix ./...

# Tidy dependencies
tidy:
	@echo "Tidying go.mod and go.sum..."
	@go mod tidy

# Update dependencies to their latest minor/patch versions
update:
	@echo "Updating go modules..."
	@go get -u ./...
	@go mod tidy

# Run fmt, vet, tidy, a go-fix modernization diff check, and tests (mirrors CI's
# validate job, minus lint — run 'make lint' separately)
check: fmt vet tidy test
	@echo "Checking go modernization (go fix -diff)..."
	@diff="$$(go fix -diff ./...)"; \
	if [ -n "$$diff" ]; then \
		echo "$$diff"; \
		echo "Modernization suggestions found. Run 'make modernize' to apply."; \
		exit 1; \
	fi
	@echo "All checks passed!"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf bin/
	@rm -rf tests/
	@rm -rf dist/
	@echo "Clean complete"

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	@docker build --build-arg XK6_VERSION=$(XK6_VERSION) -t $(IMAGE_NAME):latest .
	@echo "Docker image built: $(IMAGE_NAME):latest"

# Clean Docker image
docker-clean:
	@echo "Removing Docker image..."
	@docker rmi $(IMAGE_NAME):latest || true
	@echo "Docker image removed"

# Start docker compose services (ClickHouse and Grafana)
docker-compose-up:
	@echo "Starting docker-compose services..."
	@docker compose up -d clickhouse grafana
	@echo "Services started. ClickHouse: http://localhost:8123, Grafana: http://localhost:3000"

# Stop and remove docker-compose services
docker-compose-down:
	@echo "Stopping docker-compose services..."
	@docker compose down
	@echo "Services stopped"

# View logs from docker-compose services
docker-compose-logs:
	@docker compose logs -f

# Start docker compose services for development (profile: dev)
docker-dev:
	@echo "Starting development environment..."
	@docker compose --profile dev up --build

# Run examples/simple.js against ClickHouse in docker compose, then fail unless
# test-validator finds the written samples. Steps run one by one because
# 'up --exit-code-from' stops ClickHouse as soon as k6 exits.
docker-test:
	@echo "Running tests in docker compose..."
	@docker compose --profile test build k6-test
	@status=0; \
	docker compose --profile test run --rm k6-test && \
	docker compose --profile test run --rm --no-deps test-validator || status=$$?; \
	docker compose --profile test down; \
	exit $$status

# Clean all docker compose resources (volumes, orphans) for this project
docker-clean-all:
	@echo "Cleaning all docker compose resources..."
	@docker compose down -v --remove-orphans
	@echo "Docker cleanup complete"

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@$(GOLANGCI_LINT_VERSION)
	@go install go.k6.io/xk6/cmd/xk6@$(XK6_VERSION)
	@echo "Tools installed"

# Show help
help:
	@echo "Available targets:"
	@echo ""
	@echo "Development:"
	@echo "  make build                - Build k6 binary with xk6-output-clickhouse extension"
	@echo "  make test                 - Run all tests (includes integration tests, requires Docker)"
	@echo "  make test-unit            - Run unit tests only (no Docker required)"
	@echo "  make test-coverage        - Run tests with coverage report"
	@echo "  make fmt                  - Format code"
	@echo "  make lint                 - Run golangci-lint"
	@echo "  make vet                  - Run go vet"
	@echo "  make modernize            - Apply Go modernizers (go fix)"
	@echo "  make tidy                 - Tidy go.mod and go.sum"
	@echo "  make update               - Update dependencies to latest versions"
	@echo "  make check                - Run fmt, vet, tidy, modernization diff, and tests"
	@echo "  make clean                - Remove build artifacts"
	@echo "  make install-tools        - Install development tools"
	@echo "  make all                  - Run checks and build (default)"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build         - Build Docker image"
	@echo "  make docker-clean         - Remove Docker image"
	@echo "  make docker-compose-up    - Start ClickHouse and Grafana services"
	@echo "  make docker-compose-down  - Stop and remove all services"
	@echo "  make docker-compose-logs  - View logs from services"
	@echo "  make docker-dev           - Start development environment (profile: dev)"
	@echo "  make docker-test          - Run integration tests in docker compose (profile: test)"
	@echo "  make docker-clean-all     - Clean all docker resources (volumes, orphans)"
	@echo ""
	@echo "Variables:"
	@echo "  IMAGE_NAME=$(IMAGE_NAME)"
	@echo "  VERSION=$(VERSION)"
	@echo ""
	@echo "  make help                 - Show this help message"
