package tunnel

import (
	"net"
	"strings"
	"testing"
)

func TestServiceHeaderRoundTrip(t *testing.T) {
	left, right := net.Pipe()
	t.Cleanup(func() {
		_ = left.Close()
		_ = right.Close()
	})

	errCh := make(chan error, 1)
	go func() {
		errCh <- WriteService(left, "rss.example.com")
	}()
	service, err := ReadService(right)
	if err != nil {
		t.Fatalf("ReadService() error = %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("WriteService() error = %v", err)
	}
	if service != "rss.example.com" {
		t.Fatalf("service = %q, want rss.example.com", service)
	}
}

func TestWriteServiceRejectsOversizedName(t *testing.T) {
	left, right := net.Pipe()
	defer left.Close()
	defer right.Close()
	if err := WriteService(left, strings.Repeat("x", maxServiceName+1)); err == nil {
		t.Fatal("WriteService() accepted an oversized service name")
	}
}
