.PHONY: build clean test test-coverage fmt lint vet tidy check install-tools docker-build docker-clean docker-compose-up docker-compose-down docker-compose-logs docker-compose-test help all

# Default target
all: check build

# Build the k6 binary with the extension
build:
	@echo "Building k6 with xk6-output-clickhouse..."
	@go mod download
	@xk6 build --with github.com/mkutlak/xk6-output-clickhouse=.
	@echo "Build complete: ./k6"

# Run tests
test:
	@echo "Running tests..."
	@go test -v -race ./...

# Run tests with coverage
test-coverage:
	@echo "Running tests with coverage..."
	@go test -v -race -coverprofile=coverage.out -covermode=atomic ./...
	@go tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report generated: coverage.html"

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

# Run all checks (format, vet, lint, test)
check: fmt vet tidy test
	@echo "All checks passed!"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@rm -f k6
	@rm -f coverage.out coverage.html
	@echo "Clean complete"

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	@docker build -t xk6-output-clickhouse:latest .
	@echo "Docker image built: xk6-output-clickhouse:latest"

# Clean Docker image
docker-clean:
	@echo "Removing Docker image..."
	@docker rmi xk6-output-clickhouse:latest || true
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

# Install development tools
install-tools:
	@echo "Installing development tools..."
	@go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@go install go.k6.io/xk6/cmd/xk6@latest
	@echo "Tools installed"

# Show help
help:
	@echo "Available targets:"
	@echo "  make build                - Build k6 binary with xk6-output-clickhouse extension"
	@echo "  make test                 - Run tests"
	@echo "  make test-coverage        - Run tests with coverage report"
	@echo "  make fmt                  - Format code"
	@echo "  make lint                 - Run golangci-lint"
	@echo "  make vet                  - Run go vet"
	@echo "  make tidy                 - Tidy go.mod and go.sum"
	@echo "  make check                - Run all checks (fmt, vet, tidy, test)"
	@echo "  make clean                - Remove build artifacts"
	@echo "  make docker-build         - Build Docker image"
	@echo "  make docker-clean         - Remove Docker image"
	@echo "  make docker-compose-up    - Start ClickHouse and Grafana services"
	@echo "  make docker-compose-down  - Stop and remove all services"
	@echo "  make docker-compose-logs  - View logs from services"
	@echo "  make docker-compose-test  - Build and run k6 test with docker-compose"
	@echo "  make install-tools        - Install development tools"
	@echo "  make all                  - Run checks and build (default)"
	@echo "  make help                 - Show this help message"
