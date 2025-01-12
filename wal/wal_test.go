package wal

import (
	"bytes"
	"crypto/rand"
	"os"
	"path/filepath"
	"testing"
)

func setupTempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "wal_test")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	return dir
}

func cleanupTempDir(t *testing.T, dir string) {
	t.Helper()
	if err := os.RemoveAll(dir); err != nil {
		t.Fatalf("Failed to clean up temp directory: %v", err)
	}
}

func TestWalWriteAndRead(t *testing.T) {
	dir := setupTempDir(t)
	defer cleanupTempDir(t, dir)

	w := NewWal(dir, 1024)

	data := []byte("test data")
	if err := w.Write(data); err != nil {
		t.Fatalf("Failed to write to WAL: %v", err)
	}

	w.Sync()

	readData, err := w.Read()
	if err != nil {
		t.Fatalf("Failed to read from WAL: %v", err)
	}

	if len(readData) != 1 || !bytes.Equal(readData[0], data) {
		t.Fatalf("Read data mismatch. Expected: %s, Got: %s", data, readData[0])
	}
}

func TestLogRotation(t *testing.T) {
	dir := setupTempDir(t)
	defer cleanupTempDir(t, dir)

	w := NewWal(dir, 1)

	data := make([]byte, 1024)
	if err := w.Write(data); err != nil {
		t.Fatalf("Failed to write to WAL: %v", err)
	}

	if err := w.Write(data); err != nil {
		t.Fatalf("Failed to write to WAL: %v", err)
	}

	files, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("Failed to read WAL directory: %v", err)
	}

	var logCount int
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".db" {
			logCount++
		}
	}

	if logCount < 2 {
		t.Fatalf("Log rotation failed. Expected multiple log files, got: %d", logCount)
	}
}

func TestRepair(t *testing.T) {
	dir := setupTempDir(t)
	defer cleanupTempDir(t, dir)

	w := NewWal(dir, 1024)

	data := []byte("valid data")
	if err := w.Write(data); err != nil {
		t.Fatalf("Failed to write to WAL: %v", err)
	}

	w.Sync()

	logFile, err := os.OpenFile(filepath.Join(dir, "wal@1.db"), os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		t.Fatalf("Failed to open log file: %v", err)
	}
	defer logFile.Close()

	if _, err := logFile.Write([]byte("corrupted data")); err != nil {
		t.Fatalf("Failed to corrupt log file: %v", err)
	}

	if err := w.Repair(); err != nil {
		t.Fatalf("Repair failed: %v", err)
	}

	readData, err := w.Read()
	if err != nil {
		t.Fatalf("Failed to read from WAL after repair: %v", err)
	}

	if len(readData) != 1 || !bytes.Equal(readData[0], data) {
		t.Fatalf("Repair failed to recover valid data. Expected: %s, Got: %s", data, readData[0])
	}
}

func TestWalCheckpoint(t *testing.T) {
	dir := setupTempDir(t)
	defer cleanupTempDir(t, dir)

	w := NewWal(dir, 1024)

	segment1Data := [][]byte{
		make([]byte, 500*1024),
		make([]byte, 400*1024),
	}

	for _, data := range segment1Data {
		if err := w.Write(data); err != nil {
			t.Fatalf("Failed to write data to WAL %v", err)
		}
	}

	w.Sync()

	segment2Data := [][]byte{
		make([]byte, 512*1024),
	}

	for _, data := range segment2Data {
		if err := w.Write(data); err != nil {
			t.Fatalf("Failed to write data to WAL %v", err)
		}
	}

	if err := w.CreateCheckpoint(); err != nil {
		t.Fatalf("Failed to create checkpoint: %v", err)
	}

	w.Sync()

	segment3Data := [][]byte{
		make([]byte, 600*1024),
		make([]byte, 400*1024),
	}

	for _, data := range segment3Data {
		if err := w.Write(data); err != nil {
			t.Fatalf("Failed to write data to WAL %v", err)
		}
	}

	w.Sync()

	recoveredData, err := w.ReadFromLastCheckpoint()

	if err != nil {
		t.Fatalf("Failed to read data from last checkpoint: %v", err)
	}

	expectedData := segment3Data

	if len(recoveredData) != len(expectedData) {
		t.Fatalf("Recovered data length mismatch. Got %d, expected %d", len(recoveredData), len(expectedData))
	}

	for i, data := range recoveredData {
		if string(data) != string(expectedData[i]) {
			t.Errorf("Recovered data mismatch at index %d. Got %q, expected %q", i, data, expectedData[i])
		}
	}
}

func BenchmarkWalWrite(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	defer os.RemoveAll(dir)

	w := NewWal(dir, 2*1024)

	data := make([][]byte, 1000)
	for i := 0; i < len(data); i++ {
		data[i] = make([]byte, 1024)
		rand.Read(data[i])
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := w.Write(data[i%len(data)]); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}

func BenchmarkWalRead(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	defer os.RemoveAll(dir)

	w := NewWal(dir, 2*1024)

	data := make([][]byte, 1000)
	for i := 0; i < len(data); i++ {
		data[i] = make([]byte, 1024)
		rand.Read(data[i])
		if err := w.Write(data[i]); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}

	w.Sync()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := w.Read(); err != nil {
			b.Fatalf("Read failed: %v", err)
		}
	}
}

func BenchmarkWalRotateLog(b *testing.B) {
	dir, err := os.MkdirTemp("", "wal_benchmark")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	defer os.RemoveAll(dir)

	w := NewWal(dir, 2*1024)

	data := make([][]byte, 100)
	for i := 0; i < len(data); i++ {
		data[i] = make([]byte, 128*1024)
		rand.Read(data[i])
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if err := w.Write(data[i%len(data)]); err != nil {
			b.Fatalf("Write failed: %v", err)
		}
	}
}
