package listener

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTCPAcceptAndEstablish(t *testing.T) {
	l, err := NewTCP(TCPConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	seen := make(chan string, 1)
	done := make(chan error, 1)
	go func() {
		done <- l.Serve(ctx, func(id string, c net.Conn) {
			seen <- id
			_, _ = c.Write([]byte("hello"))
			_ = c.Close()
		})
	}()

	c, err := net.Dial("tcp", l.Addr())
	require.NoError(t, err)
	buf := make([]byte, 5)
	_, err = c.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(buf))
	assert.Equal(t, "tcp-1", <-seen)
	_ = c.Close()

	require.NoError(t, l.Close())
	require.NoError(t, <-done)
}

func TestTCPMaxConnections(t *testing.T) {
	l, err := NewTCP(TCPConfig{Address: "127.0.0.1:0", MaxConnections: 1})
	require.NoError(t, err)
	defer func() { _ = l.Close() }()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	release := make(chan struct{})
	go func() {
		_ = l.Serve(ctx, func(_ string, c net.Conn) {
			<-release
			_ = c.Close()
		})
	}()

	held, err := net.Dial("tcp", l.Addr())
	require.NoError(t, err)
	defer func() { _ = held.Close() }()

	// The second connection is accepted by the kernel then closed
	// immediately by the listener: a read returns EOF.
	require.Eventually(t, func() bool {
		return l.AcceptCount() == 1
	}, 2*time.Second, 10*time.Millisecond)
	rejected, err := net.Dial("tcp", l.Addr())
	require.NoError(t, err)
	defer func() { _ = rejected.Close() }()
	_ = rejected.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = rejected.Read(make([]byte, 1))
	require.Error(t, err)

	close(release)
}

func TestTCPGracefulCloseDrains(t *testing.T) {
	l, err := NewTCP(TCPConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)

	ctx := context.Background()
	served := make(chan error, 1)
	var established atomic.Int64
	var released atomic.Bool
	go func() {
		served <- l.Serve(ctx, func(_ string, c net.Conn) {
			established.Add(1)
			<-time.After(50 * time.Millisecond) // simulate in-flight work
			released.Store(true)
			_ = c.Close()
		})
	}()

	c, err := net.Dial("tcp", l.Addr())
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	require.Eventually(t, func() bool { return established.Load() == 1 },
		2*time.Second, 10*time.Millisecond)

	// Close waits for the in-flight connection goroutine to finish.
	require.NoError(t, l.Close())
	assert.True(t, released.Load())
	require.NoError(t, <-served)
}

func TestTCPCloseForcesIdleConnections(t *testing.T) {
	l, err := NewTCP(TCPConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)

	ctx := context.Background()
	var once sync.Once
	blocked := make(chan struct{})
	served := make(chan error, 1)
	go func() {
		served <- l.Serve(ctx, func(_ string, c net.Conn) {
			once.Do(func() { close(blocked) })
			// Block on a read; Close must force the connection shut.
			_, _ = c.Read(make([]byte, 1))
			_ = c.Close()
		})
	}()

	c, err := net.Dial("tcp", l.Addr())
	require.NoError(t, err)
	defer func() { _ = c.Close() }()
	<-blocked

	// Close closes accepted connections, unblocking the goroutine.
	require.NoError(t, l.Close())
	require.NoError(t, <-served)
	assert.EqualValues(t, 0, l.AcceptCount())
}

func TestTCPServeContextCancel(t *testing.T) {
	l, err := NewTCP(TCPConfig{Address: "127.0.0.1:0"})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	served := make(chan error, 1)
	go func() {
		served <- l.Serve(ctx, func(_ string, c net.Conn) { _ = c.Close() })
	}()
	cancel()
	require.NoError(t, <-served)
}
