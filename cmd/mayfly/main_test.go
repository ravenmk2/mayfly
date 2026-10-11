package main

import (
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/natefinch/lumberjack.v2"

	"github.com/ravenmk2/mayfly/internal/config"
)

func TestLogOutputStdStreams(t *testing.T) {
	assert.Same(t, os.Stderr, logOutput(""))
	assert.Same(t, os.Stderr, logOutput("stderr"))
	assert.Same(t, os.Stdout, logOutput("stdout"))
}

func TestNewLoggerFileOutput(t *testing.T) {
	path := filepath.Join(t.TempDir(), "mayfly.log")
	logger, closeLog := newLogger(config.LogConfig{Level: "info", Format: "json", Output: path})
	require.NotNil(t, closeLog)
	t.Cleanup(func() { _ = closeLog.Close() })
	logger.Info("hello", "key", "value")

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var record map[string]any
	require.NoError(t, json.Unmarshal([]byte(strings.TrimSpace(string(data))), &record))
	assert.Equal(t, "hello", record["msg"])
	assert.Equal(t, "value", record["key"])
}

// TestLogOutputRotation writes past one lumberjack segment and expects a
// rotated backup next to the live file.
func TestLogOutputRotation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "mayfly.log")

	w := logOutput(path)
	lj, ok := w.(*lumberjack.Logger)
	require.True(t, ok, "file output must be a rotating lumberjack writer")
	lj.MaxSize = 1 // MiB

	logger := slog.New(slog.NewTextHandler(w, nil))
	pad := strings.Repeat("x", 4096)
	for i := range 300 {
		logger.Info("rotation filler", "n", i, "pad", pad) // ≈1.2 MiB total
	}
	require.NoError(t, lj.Close())

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 2, "expected the live log plus at least one rotated backup")

	data, err := os.ReadFile(path)
	require.NoError(t, err)
	assert.Contains(t, string(data), "rotation filler")
}
