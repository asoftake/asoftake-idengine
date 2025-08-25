## Project Overview

This is ASOFTAKE ID Engine - a high-performance distributed ID generator library built with Java 24. The codebase leverages cutting-edge JDK features including sealed interfaces, pattern matching, virtual threads, and records.

## Development Commands

**Maven:**
```xml
<dependency>
    <groupId>uk.asoftake</groupId>
    <artifactId>asoftake-idengine</artifactId>
    <version>2.0.1</version>
</dependency>
```

### Quality Checks
The project relies on Java 24's compiler checks and modern language features for code quality. No formal linting tools are configured.

## Architecture Overview

### Core Design Patterns
- **Sealed Interface Architecture**: `IdGenerator` is a sealed interface with restricted implementations
- **Strategy Pattern**: Multiple ID generation algorithms through unified interface
- **Factory Pattern**: `GeneratorFactory` manages generator lifecycle and instances
- **High-Performance Concurrency**: Extensive use of atomic operations and lock-free algorithms

### Supported Algorithms
- `OPTIMIZED_SNOWFLAKE` (recommended for high throughput)
- `SNOWFLAKE` (standard implementation)
- `UUID`, `TIMESTAMP_RANDOM`, `DATABASE_SEQUENCE`

### Technology Stack
- **Java 24** with virtual threads, sealed interfaces, and pattern matching
- **Jakarta Persistence 3.1.0** for JPA integration
- **Atomic Operations** for thread-safe performance
- **Spring Boot** auto-configuration support
- **Multi-database** support (MySQL, PostgreSQL, Oracle)

## Configuration Patterns

### Spring Boot Integration
```yaml
asoftake:
  id-engine:
    algorithm: OPTIMIZED_SNOWFLAKE
    enable-metrics: true
```

## Security Features

- Package whitelisting for entity ID generation
- Comprehensive input validation
- Clock backward protection for Snowflake algorithms

## Performance Characteristics

- Single machine capability: 4M+ IDs per second
- Lock-free algorithms using packed atomic state
- Virtual thread-based background operations
- Built-in metrics collection and health monitoring

## Testing Approach

No formal test framework is configured. Testing is done manually or through external projects. Basic validation can be performed with:

```java
var generator = GeneratorFactory.getDefault();
long id = generator.nextId();
boolean isValid = generator.validate(id);
```
