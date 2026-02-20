.PHONY: build clean test test-coverage fmt lint vet tidy check install-tools docker-build docker-clean docker-compose-up docker-compose-down docker-compose-logs docker-compose-test docker-dev docker-test docker-ci docker-clean-all release-binaries docker-build-multi docker-push docker-tag checksums help all

# Project variables
REPO_OWNER ?= mkutlak
REPO_NAME ?= xk6-output-clickhouse
EXTENSION_MODULE ?= github.com/$(REPO_OWNER)/$(REPO_NAME)
XK6_VERSION ?= latest

# CI/CD variables
IMAGE_NAME ?= ghcr.io/$(REPO_OWNER)/$(REPO_NAME)
VERSION ?= latest

# Default target
all: check build

# Build the k6 binary with the extension
build:
	@echo "Building k6 with $(REPO_NAME)..."
	@go mod download
	@mkdir -p bin/
	@go run go.k6.io/xk6/cmd/xk6@$(XK6_VERSION) build --output bin/k6 --with $(EXTENSION_MODULE)=.
	@echo "Build complete: ./bin/k6"

# Run tests
test:
	@echo "Running tests..."
	@go test -v -race ./...

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

# Tidy dependencies
tidy:
	@echo "Tidying go.mod and go.sum..."
	@go mod tidy

# Update dependencies to their latest minor/patch versions
update:
	@echo "Updating go modules..."
	@go get -u ./...
	@go mod tidy

# Run all checks (format, vet, lint, test)
check: fmt vet tidy test
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
	@docker build -t $(IMAGE_NAME):latest .
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

# Run k6 test with docker-compose
docker-compose-test:
	@echo "Building k6 image and running test..."
	@docker compose build k6
	@docker compose --profile test run --rm k6
	@echo "Test completed"

# Start docker compose services for development (profile: dev)
docker-dev:
	@echo "Starting development environment..."
	@docker compose --profile dev up --build

# Run tests in docker compose (profile: test)
docker-test:
	@echo "Running tests in docker compose..."
	@docker compose --profile test up --build --abort-on-container-exit --exit-code-from k6-test

# Run CI pipeline in docker compose (profile: ci)
docker-ci:
	@echo "Running CI pipeline..."
	@docker compose --profile ci up --build --abort-on-container-exit
	@docker compose --profile ci run test-validator

# Clean all docker compose resources (volumes, orphans)
docker-clean-all:
	@echo "Cleaning all docker compose resources..."
	@docker compose down -v --remove-orphans
	@docker system prune -f
	@echo "Docker cleanup complete"

# Build release binaries for multiple architectures
release-binaries: check
	@echo "Building release binaries..."
	@mkdir -p dist
	@for os in linux; do \
		for arch in amd64 arm64; do \
			echo "Building k6 for $$os/$$arch..."; \
			GOOS=$$os GOARCH=$$arch GOPRIVATE="go.k6.io/k6" go run go.k6.io/xk6/cmd/xk6@$(XK6_VERSION) build \
				--output ./dist/k6-$$os-$$arch \
				--with $(EXTENSION_MODULE)=.; \
			sha256sum ./dist/k6-$$os-$$arch > ./dist/k6-$$os-$$arch.sha256; \
		done; \
	done
	@echo "Release binaries built in ./dist"
	@ls -lh ./dist/k6-*

# Build multi-arch Docker image with buildx
docker-build-multi:
	@echo "Building multi-architecture Docker image..."
	@docker buildx build \
		--platform linux/amd64,linux/arm64 \
		--tag $(IMAGE_NAME):$(VERSION) \
		--push \
		.
	@echo "Multi-arch image pushed: $(IMAGE_NAME):$(VERSION)"

# Push Docker image to registry
docker-push:
	@echo "Pushing Docker image..."
	@docker push $(IMAGE_NAME):$(VERSION)
	@echo "Pushed: $(IMAGE_NAME):$(VERSION)"

# Tag Docker image with version
docker-tag:
	@echo "Tagging Docker image..."
	@docker tag $(IMAGE_NAME):$(shell git rev-parse --short HEAD) $(IMAGE_NAME):$(VERSION)
	@echo "Tagged: $(IMAGE_NAME):$(VERSION)"

# Generate SHA256 checksums for release binaries
checksums:
	@echo "Generating checksums..."
	@cd dist && sha256sum k6-* > checksums.txt
	@echo "Checksums generated: dist/checksums.txt"

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install go.k6.io/xk6/cmd/xk6@$(XK6_VERSION)
	@echo "Tools installed"

# Show help
help:
	@echo "Available targets:"
	@echo ""
	@echo "Development:"
	@echo "  make build                - Build k6 binary with xk6-output-clickhouse extension"
	@echo "  make test                 - Run tests"
	@echo "  make test-coverage        - Run tests with coverage report"
	@echo "  make fmt                  - Format code"
	@echo "  make lint                 - Run golangci-lint"
	@echo "  make vet                  - Run go vet"
	@echo "  make tidy                 - Tidy go.mod and go.sum"
	@echo "  make update               - Update dependencies to latest versions"
	@echo "  make check                - Run all checks (fmt, vet, tidy, test)"
	@echo "  make clean                - Remove build artifacts"
	@echo "  make install-tools        - Install development tools"
	@echo "  make all                  - Run checks and build (default)"
	@echo ""
	@echo "Docker:"
	@echo "  make docker-build         - Build Docker image"
	@echo "  make docker-build-multi   - Build and push multi-arch Docker image"
	@echo "  make docker-push          - Push Docker image to registry"
	@echo "  make docker-tag           - Tag Docker image with VERSION"
	@echo "  make docker-clean         - Remove Docker image"
	@echo "  make docker-compose-up    - Start ClickHouse and Grafana services"
	@echo "  make docker-compose-down  - Stop and remove all services"
	@echo "  make docker-compose-logs  - View logs from services"
	@echo "  make docker-compose-test  - Build and run k6 test with docker-compose"
	@echo "  make docker-dev           - Start development environment (profile: dev)"
	@echo "  make docker-test          - Run tests in docker compose (profile: test)"
	@echo "  make docker-ci            - Run CI pipeline (profile: ci)"
	@echo "  make docker-clean-all     - Clean all docker resources (volumes, orphans)"
	@echo ""
	@echo "Release (CI/CD):"
	@echo "  make release-binaries     - Build release binaries for amd64 and arm64"
	@echo "  make checksums            - Generate SHA256 checksums for binaries"
	@echo ""
	@echo "Variables:"
	@echo "  IMAGE_NAME=$(IMAGE_NAME)"
	@echo "  VERSION=$(VERSION)"
	@echo ""
	@echo "  make help                 - Show this help message"
