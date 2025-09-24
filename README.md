# Wildberries Parser Service (Thesis)

This repository contains the Wildberries parser microservice for the thesis project: "Development of an analytics system for marketplace product grids using neural network technologies for automation and acceleration of assortment analysis".

## Purpose & Architecture

The parser-wb service collects product data from the Wildberries marketplace, providing up-to-date information for analysis. It is implemented in Rust and integrates with the internal API and database services.

### Main Components
- `src/main.rs`: Main logic for scraping, DB connection, and task handling
- `Cargo.toml`: Rust dependencies
- `docker-compose.yml`, `Dockerfile`: Containerization scripts
- `.env-clear`: Example environment configuration for DB connection

## Workflow
1. Connects to Wildberries API and scrapes product data
2. Stores and updates product info in PostgreSQL
3. Communicates with internal API via gRPC for parsing and enrichment
4. Handles concurrent data processing and error handling

## Features
- Wildberries product data scraping
- Integration with PostgreSQL
- gRPC communication with internal API
- Asynchronous and concurrent data processing
- Dockerized deployment
- Error handling and logging

## Tech Stack
- Rust
- reqwest (HTTP client)
- deadpool-postgres (connection pool)
- Docker for containerization

## Environment Setup
1. Build and run with Docker:
   ```powershell
   docker build -t parser-wb .
   docker run parser-wb
   ```
   Or use `docker-compose` if available.
2. Manual launch (development):
   ```powershell
   cargo build --release
   .\target\release\parser-wb
   ```

## Configuration
- All connection details configured in `.env` files of other repos
- Ensure the database and internal API are running before starting the parser

## Integration
- Communicates with internal API and DB services
- All connection details configured in `.env` files of other repos

## Development Notes
- Rust, reqwest, deadpool-postgres
- See thesis for scraping logic, concurrency, and data flow
