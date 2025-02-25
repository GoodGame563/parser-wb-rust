FROM rust:1.84.1

RUN rustup target add x86_64-unknown-linux-musl
RUN apt update && apt install -y musl-tools musl-dev
RUN apt-get install -y build-essential
RUN yes | apt install gcc-x86-64-linux-gnu

WORKDIR /app

COPY . /app
ENV RUSTFLAGS='-C linker=x86_64-linux-gnu-gcc'
RUN cargo build --target x86_64-unknown-linux-musl --release
RUN cargo install cargo-watch


# CMD cargo run