PB = $(wildcard *.proto)
GO = $(PB:.proto=.pb.go)

all: $(GO)

build:
	protoc -I=. -I=${GOPATH}/src -I=${GOPATH}/src/github.com/gogo/protobuf/protobuf --gogo_out=. --proto_path=../../go-libp2p-record/pb/ --proto_path=./ dht.proto

%.pb.go: %.proto
		protoc --proto_path=$(GOPATH)/src:. --gogofast_out=. $<

clean:
		rm -f *.pb.go