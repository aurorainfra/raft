module github.com/hashicorp/raft

go 1.25.0

retract v1.1.3 // Deleted original tag; module checksum may not be accurate.

require (
	github.com/hashicorp/go-hclog v1.6.3
	github.com/hashicorp/go-metrics v0.7.0
	github.com/hashicorp/go-msgpack/v2 v2.1.5
	github.com/ipfs/go-cid v0.0.6
	github.com/stretchr/testify v1.11.1
	github.com/whyrusleeping/cbor-gen v0.4.2
	golang.org/x/xerrors v0.0.0-20191204190536-9bdfabe68543
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/hashicorp/golang-lru v1.0.2 // indirect
	github.com/kr/pretty v0.2.1 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/minio/blake2b-simd v0.0.0-20160723061019-3f5f724cb5b1 // indirect
	github.com/minio/sha256-simd v0.1.1-0.20190913151208-6de447530771 // indirect
	github.com/mr-tron/base58 v1.1.3 // indirect
	github.com/multiformats/go-base32 v0.0.3 // indirect
	github.com/multiformats/go-base36 v0.1.0 // indirect
	github.com/multiformats/go-multibase v0.0.3 // indirect
	github.com/multiformats/go-multihash v0.0.13 // indirect
	github.com/multiformats/go-varint v0.0.5 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/spaolacci/murmur3 v1.1.0 // indirect
	golang.org/x/crypto v0.0.0-20190611184440-5c40567a22f8 // indirect
	golang.org/x/sys v0.47.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/whyrusleeping/cbor-gen => github.com/aurorainfra/cbor-gen v0.4.2
