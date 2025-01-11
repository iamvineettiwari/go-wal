package wal

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const fileExt = "db"

var (
	ErrBrokenData = errors.New("broken data in log")
	ErrRead       = errors.New("something went wrong while reading log file")
)

const (
	syncInterval = 100 * time.Millisecond
	filePrefix   = "wal"
)

type Wal struct {
	dataDirectory string
	logFile       *os.File
	writer        *bufio.Writer
	maxLogSize    int32
	syncTimer     *time.Ticker

	lock           *sync.RWMutex
	lastSegmentNo  int32
	lastSequenceNo int32
}

func NewWal(directory string, maxLogSizeInKB int32) *Wal {

	if err := os.MkdirAll(directory, 0644); err != nil {
		panic(fmt.Sprintf("Unable to create directory - %s", directory))
	}

	file, lastSegmentNo := getLastLogFile(directory)

	if _, err := file.Seek(0, io.SeekEnd); err != nil {
		panic(fmt.Sprintf("Unable to read log file - %s", file.Name()))
	}

	wal := &Wal{
		dataDirectory:  directory,
		logFile:        file,
		writer:         bufio.NewWriter(file),
		maxLogSize:     maxLogSizeInKB * 1024,
		syncTimer:      time.NewTicker(syncInterval),
		lock:           &sync.RWMutex{},
		lastSegmentNo:  lastSegmentNo,
		lastSequenceNo: 0,
	}

	data, err := getLastLogData("", file.Name())

	if data != nil {
		wal.lastSequenceNo = data.GetLastSequence()
	}

	if err != nil {
		wal.Repair()
	}

	go wal.applySyncIntervally()

	return wal
}

func (w *Wal) Write(data []byte) error {
	return w.writeData(data)
}

func (w *Wal) Read() ([][]byte, error) {
	w.lock.RLock()
	segmentNo := w.lastSegmentNo
	w.lock.RUnlock()

	return w.readData(segmentNo)
}

func (w *Wal) ReadFromSegment(segmentNo int32) ([][]byte, error) {
	data := [][]byte{}

	if segmentNo < 1 {
		return data, errors.New("segment no must start from 1")
	}

	w.lock.RLock()
	totalSegment := w.lastSegmentNo
	w.lock.RUnlock()

	for i := segmentNo; i <= totalSegment; i++ {
		curData, err := w.readData(i)

		if err != nil {
			return data, err
		}

		data = append(data, curData...)
	}

	return data, nil
}

func (w *Wal) Repair() error {
	if err := w.Sync(); err != nil {
		return err
	}

	for i := 1; i <= int(w.lastSegmentNo); i++ {
		if err := w.repairSegment(int32(i)); err != nil {
			return err
		}
	}

	w.syncTimer.Reset(syncInterval)
	return nil
}

func (w *Wal) Sync() error {
	if err := w.writer.Flush(); err != nil {
		return err
	}

	return nil
}

func (w *Wal) writeData(data []byte) error {
	w.lock.Lock()
	defer w.lock.Unlock()

	w.lastSequenceNo += 1
	walData := NewWalData(w.lastSequenceNo, data)
	byteData, err := json.Marshal(walData)

	if err != nil {
		return err
	}

	size, err := intToBytes(int32(len(byteData)), binary.LittleEndian)

	if err != nil {
		return err
	}

	err = w.checkAndRotateLog(len(byteData) + len(size))

	if err != nil {
		return err
	}

	byteData = append(size, byteData...)

	_, err = w.writer.Write(byteData)

	if err != nil {
		return err
	}

	return nil
}

func (w *Wal) readData(segmentNo int32) ([][]byte, error) {
	walDataSet, err := w.readSegment(segmentNo)

	if err != nil {
		return nil, err
	}

	data := make([][]byte, 0, len(walDataSet))

	for _, item := range walDataSet {
		data = append(data, item.GetData())
	}

	return data, nil
}

func (w *Wal) readSegment(segmentNo int32) ([]*WalData, error) {
	segFileName := getLogFileName(segmentNo)
	filePath := filepath.Join(w.dataDirectory, segFileName)
	segFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, 0644)

	if err != nil {
		return nil, err
	}

	defer segFile.Close()

	if _, err := segFile.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	data := []*WalData{}

	for {
		var size int32
		err = binary.Read(segFile, binary.LittleEndian, &size)

		if err != nil {
			if err == io.EOF {
				break
			}

			return nil, ErrRead
		}

		byteData := make([]byte, size)

		if _, err := io.ReadFull(segFile, byteData); err != nil {
			return nil, ErrRead
		}

		var curData *WalData

		if err := json.Unmarshal(byteData, &curData); err != nil {
			return nil, errors.New("failed to load data from log")
		}

		if !curData.IsValid() {
			return nil, ErrBrokenData
		}

		data = append(data, curData)
	}

	return data, nil
}

func (w *Wal) repairSegment(segmentNo int32) error {
	segFileName := getLogFileName(segmentNo)
	filePath := filepath.Join(w.dataDirectory, segFileName)
	segFile, err := os.OpenFile(filePath, os.O_CREATE|os.O_RDONLY, 0644)

	if err != nil {
		return err
	}

	defer segFile.Close()

	data := []byte{}
	lastData := &WalData{}

	for {
		var size int32

		err = binary.Read(segFile, binary.LittleEndian, &size)

		if err != nil && err == io.EOF {
			break
		}

		byteData := make([]byte, size)

		_, err = io.ReadFull(segFile, byteData)

		if err != nil {
			break
		}

		walData := &WalData{}

		if err := json.Unmarshal(byteData, &walData); err != nil {
			break
		}

		if !walData.IsValid() {
			break
		}

		curSize, _ := intToBytes(int32(len(byteData)), binary.LittleEndian)

		data = append(data, curSize...)
		data = append(data, byteData...)
		json.Unmarshal(byteData, &lastData)
	}

	segFileNameNew := getLogFileName(segmentNo)
	segFileNewPath := filepath.Join(w.dataDirectory, fmt.Sprintf("%s.tmp", segFileNameNew))
	newSegFile, err := os.OpenFile(segFileNewPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		return err
	}

	if _, err := newSegFile.Write(data); err != nil {
		return err
	}

	if err := os.Rename(segFileNewPath, filepath.Join(w.dataDirectory, segFileNameNew)); err != nil {
		return err
	}

	if segmentNo == w.lastSegmentNo {
		w.logFile = newSegFile
		w.writer = bufio.NewWriter(newSegFile)

		if lastData != nil {
			w.lastSequenceNo = lastData.GetLastSequence()
		}
	}

	return nil
}

func (w *Wal) applySyncIntervally() {
	for range w.syncTimer.C {
		w.lock.Lock()
		w.Sync()
		w.lock.Unlock()
	}
}

func (w *Wal) checkAndRotateLog(newSize int) error {
	info, err := w.logFile.Stat()

	if err != nil {
		return err
	}

	if int(info.Size())+newSize > int(w.maxLogSize) {
		return w.rotateLog()
	}

	return nil
}

func (w *Wal) rotateLog() error {
	err := w.Sync()

	if err != nil {
		return err
	}

	newSegmentNo := w.lastSegmentNo + 1
	newLogFile := getLogFileName(newSegmentNo)

	filePath := filepath.Join(w.dataDirectory, newLogFile)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		return err
	}

	w.lastSegmentNo = newSegmentNo
	w.logFile = file
	w.writer = bufio.NewWriter(file)
	return nil
}

func getLastLogFile(directory string) (*os.File, int32) {
	entries, err := os.ReadDir(directory)
	if err != nil {
		panic(fmt.Sprintf("Unable to read directory - %s: %v", directory, err))
	}

	var reqFile string
	var maxSegment int32
	hasFiles := false

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		segmentNumber := getSegmentNumberFromFile(fileName)

		if segmentNumber > maxSegment {
			reqFile = fileName
			maxSegment = segmentNumber
		}

		hasFiles = true
	}

	if !hasFiles {
		reqFile = getLogFileName(1)
		maxSegment = 1
	}

	filePath := filepath.Join(directory, reqFile)
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)

	if err != nil {
		panic(fmt.Sprintf("Unable to open file - %s: %v", filePath, err))
	}

	return file, maxSegment
}

func getLastLogData(directory, filename string) (*WalData, error) {
	file, err := os.OpenFile(filepath.Join(directory, filename), os.O_RDONLY, 0644)

	if err != nil {
		return nil, fmt.Errorf("unable to read log file - %s", filename)
	}

	defer file.Close()

	var size int32
	var data *WalData
	var currentOffset int64

	for {
		var curSize int32

		err = binary.Read(file, binary.LittleEndian, &curSize)

		if err != nil && err == io.EOF {
			if currentOffset == 0 {
				return data, nil
			}

			if _, err := file.Seek(currentOffset, io.SeekStart); err != nil {
				return nil, ErrRead
			}

			byteData := make([]byte, size)

			if _, err := io.ReadFull(file, byteData); err != nil {
				return nil, ErrRead
			}

			if err := json.Unmarshal(byteData, &data); err != nil {
				return nil, errors.New("failed to load data from log")
			}

			if !data.IsValid() {
				return nil, ErrBrokenData
			}

			return data, nil

		} else if err != nil {
			return nil, ErrRead
		}

		currentOffset, err = file.Seek(0, io.SeekCurrent)

		if err != nil {
			return nil, ErrRead
		}

		size = curSize

		if _, err := file.Seek(int64(curSize), io.SeekCurrent); err != nil {
			return nil, ErrRead
		}
	}
}

func getSegmentNumberFromFile(fileName string) int32 {
	parts := strings.SplitN(fileName, "@", 2)
	if len(parts) != 2 {
		panic(fmt.Sprintf("Invalid file name format: %s", fileName))
	}

	segmentParts := strings.SplitN(parts[1], ".", 2)
	if len(segmentParts) != 2 {
		panic(fmt.Sprintf("Invalid file name format: %s", fileName))
	}

	segmentNumber, err := strconv.Atoi(segmentParts[0])
	if err != nil {
		panic(fmt.Sprintf("Invalid segment number in file name: %s", fileName))
	}

	return int32(segmentNumber)
}

func getLogFileName(segmentNo int32) string {
	return fmt.Sprintf("%s@%d.%s", filePrefix, segmentNo, fileExt)
}

func intToBytes(num int32, endianess binary.ByteOrder) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, endianess, num)

	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}
