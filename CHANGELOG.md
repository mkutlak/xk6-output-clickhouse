## [0.4.0](https://github.com/mkutlak/xk6-output-clickhouse/compare/v0.3.0...v0.4.0) (2025-12-23)

### Features

* enhance documentation and improve config validation ([1beed9a](https://github.com/mkutlak/xk6-output-clickhouse/commit/1beed9a7708aa07f25972188fb3691beb241f1fa))

## [0.3.0](https://github.com/mkutlak/xk6-output-clickhouse/compare/v0.2.0...v0.3.0) (2025-12-23)

### Features

* Add connection resilience with retry and buffering ([83e949b](https://github.com/mkutlak/xk6-output-clickhouse/commit/83e949b056ff6ad96df3eaf3724fc54cadf0ef2d))
* Add error metrics and atomic counters to output ([6538b5e](https://github.com/mkutlak/xk6-output-clickhouse/commit/6538b5e2fa39f42a9179829f644772d71c0004e8))

## [0.2.0](https://github.com/mkutlak/xk6-output-clickhouse/compare/v0.1.0...v0.2.0) (2025-12-11)

### Features

* Go module updates and modernization ([066f523](https://github.com/mkutlak/xk6-output-clickhouse/commit/066f523534f881dfbe18b0da0fbda7d567c25755))
* Update xk6 version and refactor release workflow ([3736c2e](https://github.com/mkutlak/xk6-output-clickhouse/commit/3736c2e3b2fe969e44ed931fce931414ec0cd828))

### Code Refactoring

* **clickhouse:** Restructure schema, conversions, and tests ([2d7c6ec](https://github.com/mkutlak/xk6-output-clickhouse/commit/2d7c6ecffe26674a7aed19dad93362916fee3782))

### Documentation

* add CONTRIBUTING.md guide ([9819714](https://github.com/mkutlak/xk6-output-clickhouse/commit/9819714e280b3efe06a40ddc97653a79f74c0625))

## [0.1.0](https://github.com/mkutlak/xk6-output-clickhouse/compare/v0.0.1...v0.1.0) (2025-12-10)

### Features

* Introduce pluggable schema system and rename extension ([798dadd](https://github.com/mkutlak/xk6-output-clickhouse/commit/798dadddbadb03ea7310481586a44b99fdfdfb79))

### Bug Fixes

* gosec SQL string formatting justification ([f06b51e](https://github.com/mkutlak/xk6-output-clickhouse/commit/f06b51eb73f0a2276f22b1a90dddd72dc9d85a5a))

## 1.0.0 (2025-12-06)

### Features

* Add integration tests with Testcontainers for ClickHouse output ([6e41b98](https://github.com/mkutlak/xk6-output-clickhouse/commit/6e41b98e6b57ca50963a98afdd8fc7a14b1ee41d))
* **clickhouse:** Add comprehensive TLS/SSL support with mTLS ([8f31bd3](https://github.com/mkutlak/xk6-output-clickhouse/commit/8f31bd3f7efe44ed948f75eefed185ccffb94d09))
* **clickhouse:** Add flexible schema handling and conversion ([5a257ec](https://github.com/mkutlak/xk6-output-clickhouse/commit/5a257ecb5bf9d56b05c6751ec1b9ee021aa60ff9))
* **clickhouse:** Enhance schema handling and concurrency ([6d81cc1](https://github.com/mkutlak/xk6-output-clickhouse/commit/6d81cc127dabbebe3a24523587944104e92d107d))
* **clickhouse:** Improve schema conversion and error handling ([46109b1](https://github.com/mkutlak/xk6-output-clickhouse/commit/46109b16b371dc061483be6d0f345e7548174326))
* **clickhouse:** Use context for graceful shutdown in flush operations ([6d4c536](https://github.com/mkutlak/xk6-output-clickhouse/commit/6d4c536fd747db42d9800dd4b49f9837581c3937))
* **docker:** Enable ClickHouse and Grafana services in docker-compose ([cb8b558](https://github.com/mkutlak/xk6-output-clickhouse/commit/cb8b5589a0e1e5194ccd503b323a86a4da1f18ff))
* setup CI/CD workflows and improve development tooling ([cff8ea1](https://github.com/mkutlak/xk6-output-clickhouse/commit/cff8ea165394da4999c2a8aa3496844ad4523d53))

### Bug Fixes

* **clickhouse:** Escape database and table identifiers in schema creation ([51d8a9a](https://github.com/mkutlak/xk6-output-clickhouse/commit/51d8a9ada06f9a0dbc7b90db27398b723d22bca4))
* **clickhouse:** Implement robust concurrency safety for flush mechanism ([7f6773b](https://github.com/mkutlak/xk6-output-clickhouse/commit/7f6773b33d1bc5a7e272f492891d99dc62fb3e29))

### Performance Improvements

* **clickhouse:** Add context cancellation and memory pooling ([dfebb45](https://github.com/mkutlak/xk6-output-clickhouse/commit/dfebb45f1abd55dc761399072d4073c244bf12b0))

### Code Refactoring

* **clickhouse:** Rename metric_name to metric and metric_value to value ([6a820fb](https://github.com/mkutlak/xk6-output-clickhouse/commit/6a820fbc22f0b98e88632469aeeb68d06a506c24))

### Documentation

* Document configuration defaults in config.go and README.md ([f176d64](https://github.com/mkutlak/xk6-output-clickhouse/commit/f176d64d93c07c8b792e054146c0398ce2386b09))
