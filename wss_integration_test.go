package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"frp/client"
	"frp/config"
	"frp/server"
)

func TestWSSMuxForwardsConcurrentStreams(t *testing.T) {
	echoListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer echoListener.Close()
	go serveEcho(echoListener)

	webSocketAddr := unusedAddress(t)
	forwardAddr := unusedAddress(t)
	config.C = &config.Config{
		Transport: config.TransportWSSMux,
		Token:     "integration-token",
		Server: config.ServerConfig{
			ListenAddr:        webSocketAddr,
			ControlServerName: "localhost",
			WebSocketPath:     config.DefaultWebSocketPath,
			ForwardServers: []config.ForwardServerConfig{{
				ProxyServerName: "echo.example.com",
				ListenAddr:      forwardAddr,
			}},
		},
		Client: config.ClientConfig{
			RemoteAddr:        webSocketAddr,
			ControlServerName: "localhost",
			WebSocketPath:     config.DefaultWebSocketPath,
			WebSocketScheme:   "ws",
			LocalServers: []config.LocalServerConfig{{
				ProxyServerName: "echo.example.com",
				LocalAddr:       echoListener.Addr().String(),
			}},
		},
	}

	wssServer := server.NewWSS()
	if err := wssServer.Start(); err != nil {
		t.Fatalf("server.Start() error = %v", err)
	}
	t.Cleanup(wssServer.Stop)
	assertUnauthorizedTunnelLooksAbsent(t, webSocketAddr)
	assertWrongHostLooksAbsent(t, webSocketAddr)
	wssClient := client.NewWSS()
	if err := wssClient.Start(); err != nil {
		t.Fatalf("client.Start() error = %v", err)
	}
	t.Cleanup(wssClient.Stop)

	waitForTunnel(t, webSocketAddr)

	const streams = 32
	var wg sync.WaitGroup
	errCh := make(chan error, streams)
	for index := 0; index < streams; index++ {
		index := index
		wg.Add(1)
		go func() {
			defer wg.Done()
			payload := []byte(fmt.Sprintf("stream-%02d", index))
			conn, err := net.DialTimeout("tcp", forwardAddr, 5*time.Second)
			if err != nil {
				errCh <- err
				return
			}
			defer conn.Close()
			_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
			if _, err := conn.Write(payload); err != nil {
				errCh <- err
				return
			}
			response := make([]byte, len(payload))
			if _, err := io.ReadFull(conn, response); err != nil {
				errCh <- err
				return
			}
			if string(response) != string(payload) {
				errCh <- fmt.Errorf("response %q, want %q", response, payload)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
}

func assertUnauthorizedTunnelLooksAbsent(t *testing.T, address string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, response, err := websocket.Dial(ctx, "ws://"+address+config.DefaultWebSocketPath, nil)
	if err == nil {
		t.Fatal("unauthorized WebSocket connection succeeded")
	}
	if response == nil || response.StatusCode != http.StatusNotFound {
		t.Fatalf("unauthorized status = %v, want 404", response)
	}
}

func assertWrongHostLooksAbsent(t *testing.T, address string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	header := make(http.Header)
	header.Set("Authorization", "Bearer integration-token")
	_, response, err := websocket.Dial(ctx, "ws://"+address+config.DefaultWebSocketPath, &websocket.DialOptions{
		Host:       "wrong.example.com",
		HTTPHeader: header,
	})
	if err == nil {
		t.Fatal("wrong-host WebSocket connection succeeded")
	}
	if response == nil || response.StatusCode != http.StatusNotFound {
		t.Fatalf("wrong-host status = %v, want 404", response)
	}
}

func unusedAddress(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	address := listener.Addr().String()
	if err := listener.Close(); err != nil {
		t.Fatal(err)
	}
	return address
}

func waitForTunnel(t *testing.T, address string) {
	t.Helper()
	client := &http.Client{Timeout: time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		response, err := client.Get("http://" + address + "/healthz")
		if err == nil {
			_ = response.Body.Close()
			if response.StatusCode == http.StatusOK {
				return
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("tunnel did not become healthy")
}

func serveEcho(listener net.Listener) {
	for {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		go func() {
			defer conn.Close()
			_, _ = io.Copy(conn, conn)
		}()
	}
}
