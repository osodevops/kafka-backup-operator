# Build stage
FROM rust:1.100-slim-trixie@sha256:6f14b2b4b278ec0d50aa1d69541c304ce0f95e0f6268487aa9924af1c2e9aebe AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    pkg-config \
    libssl-dev \
    cmake \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /workspace

# Copy manifests first for better caching
COPY Cargo.toml Cargo.lock ./

# Create dummy main.rs to build dependencies
RUN mkdir -p src/bin && \
    echo "fn main() {}" > src/main.rs && \
    echo "fn main() {}" > src/bin/crdgen.rs

# Build dependencies only (will be cached)
RUN cargo build --locked --release && rm -rf src

# Copy actual source code
COPY src ./src

# Touch main.rs to invalidate the cache for final build
RUN touch src/main.rs

# Build the actual binary
RUN cargo build --locked --release --bin kafka-backup-operator

# Runtime stage
FROM debian:trixie-slim@sha256:8aa5a7bf5a1955bddbe7d74fcdd0b91ab30e74362a8b73af672b0481b35b44a0

# Install runtime dependencies
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd -r -u 1000 -g root operator

# Copy the binary
COPY --from=builder /workspace/target/release/kafka-backup-operator /usr/local/bin/operator

# Set ownership and permissions
RUN chown operator:root /usr/local/bin/operator && \
    chmod 755 /usr/local/bin/operator

USER 1000

# Expose metrics port
EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/operator"]
