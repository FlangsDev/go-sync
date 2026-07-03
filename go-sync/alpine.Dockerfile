# --- Build stage ---
FROM golang:1.25-alpine AS builder

WORKDIR /app

COPY main.go ./

# Generate go.mod & go.sum otomatis di dalam image, jadi kamu tidak perlu
# install Go di mesin lokal sama sekali.

ENV GOFLAGS=-mod=mod
ENV GOPROXY=https://proxy.golang.org,direct

# Matikan verifikasi ke sum.golang.org (sering diblokir firewall/ISP -> connection
# refused). Modul tetap diunduh dari proxy.golang.org, cuma tanpa cek checksum DB.
ENV GOSUMDB=off

RUN go mod init go-sync

# Pin Docker SDK ke rilis v27 yang masih SATU modul (github.com/docker/docker).
# Rilis terbaru memecah paket api & client jadi modul terpisah yang path-nya
# sudah migrasi ke github.com/moby/moby/*, sehingga resolusi otomatis gagal
# dengan error "module declares its path as github.com/moby/moby/api".
# Dengan pin ini, semua paket api/types/* dan client diambil dari satu modul v27.
#
# go-connections HARUS di-pin ke v0.5.0 (versi yang di-vendor docker v27.5.1).
# Karena docker@v27.5.1 itu +incompatible (tanpa go.mod yang mengikat), batasan
# versi dependency-nya tidak dihormati; tanpa pin ini `go build` menarik
# go-connections v0.7.0 yang sudah menghapus sockets.DialPipe, sehingga build
# client.go gagal dengan "undefined: sockets.DialPipe".
RUN go get github.com/docker/docker@v27.5.1+incompatible && \
    go get github.com/docker/go-connections@v0.5.0 && \
    go get github.com/neo4j/neo4j-go-driver/v5@v5.28.4

# Tidak pakai `go mod tidy` — dia meresolusi SELURUH graph termasuk dependency
# test (gotest.tools, otelhttp, dsb) yang tidak dibutuhkan untuk build binary.
# `go build -mod=mod` cukup: hanya menarik yang benar-benar diimport main.go.
RUN CGO_ENABLED=0 GOOS=linux go build -o go-sync .

# --- Runtime stage ---
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/go-sync .

ENTRYPOINT ["./go-sync"]