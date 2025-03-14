package main

import (
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	DATA_FILES_DIR            = "./data"
	RESULT_FILE_PATH          = "./result.txt"
	MAX_THREADS_COUNT         = 4
	MAX_CHUNK_SIZE            = 3
	INTERVAL_MS_CHECK_FILES   = 1000 * time.Millisecond
	INTERVAL_MS_PROCESS_FILES = 1000 * time.Millisecond
)

// FileStatus represents the possible statuses of a file.
type FileStatus string

const (
	NotRead   FileStatus = "not_read"
	Reading   FileStatus = "reading"
	Completed FileStatus = "completed"
)

// FileLock represents the state of a file in the lock mechanism.
type FileLock struct {
	Status          FileStatus
	ReadLinesNumber int
}

// FileWatcherService is the main service that watches the directory and processes files.
type FileWatcherService struct {
	activeWorkers          int
	filesQueue             []string
	lock                   map[string]FileLock
	currentFileIndex       int
	watching               bool
	maxThreads             int
	intervalMsCheckFiles   time.Duration
	intervalMsProcessFiles time.Duration
	folderPath             string
	mu                     sync.Mutex
	newFilesEvent          chan string // Channel to notify new files
	processFilesEvent      chan bool   // Channel to trigger file processing
}

// NewFileWatcherService creates a new FileWatcherService instance.
func NewFileWatcherService(folderPath string, maxThreads int, intervalMsCheckFiles, intervalMsProcessFiles time.Duration) *FileWatcherService {
	return &FileWatcherService{
		folderPath:             folderPath,
		maxThreads:             maxThreads,
		intervalMsCheckFiles:   intervalMsCheckFiles,
		intervalMsProcessFiles: intervalMsProcessFiles,
		activeWorkers:          0,
		filesQueue:             []string{},
		lock:                   make(map[string]FileLock),
		currentFileIndex:       0,
		newFilesEvent:          make(chan string, 10), // Buffered channel
		processFilesEvent:      make(chan bool),       // Simple event channel
	}
}
func (s *FileWatcherService) handleNewFileEvent(file string) {
	// Check if the file is already tracked and processing
	if s.isFileTracked(file) {
		// If file is already being processed or completed, just log and return
		fmt.Printf("File %s is already being processed or completed\n", file)
		return
	}

	// Otherwise, trigger processing of the file
	fmt.Printf("Processing new file: %s\n", file)
	// Trigger file processing, which will start the worker
	s.processFiles()
}

// StartWatching starts the service to watch the directory and process files.
func (s *FileWatcherService) StartWatching() {
	if s.watching {
		return
	}
	s.watching = true

	// Event loop for new file detection
	go func() {
		for {
			select {
			case <-time.After(s.intervalMsCheckFiles):
				s.checkForNewFiles()
			case newFile := <-s.newFilesEvent:
				// Process the new file event
				s.handleNewFileEvent(newFile)
			}
		}
	}()

	// Event loop for processing files
	go func() {
		for {
			select {
			case <-time.After(s.intervalMsProcessFiles):
				s.processFiles()
			case <-s.processFilesEvent:
				// Trigger manual file processing event
				s.processFiles()
			}
		}
	}()
}

// checkForNewFiles checks the directory for new CSV files and adds them to the queue.
func (s *FileWatcherService) checkForNewFiles() {
	files, err := os.ReadDir(s.folderPath)
	if err != nil {
		fmt.Println("Error reading directory:", err)
		return
	}

	for _, file := range files {
		if file.IsDir() {
			continue
		}
		if filepath.Ext(file.Name()) == ".csv" && !s.isFileTracked(file.Name()) {
			s.lock[file.Name()] = FileLock{Status: NotRead, ReadLinesNumber: 0}
			s.filesQueue = append(s.filesQueue, file.Name())
			fmt.Printf("Added new file %s to the queue with status 'not_read'\n", file.Name())
			s.newFilesEvent <- file.Name() // Notify event of new file
		}
	}
}

// isFileTracked checks if a file is already being tracked by the service.
func (s *FileWatcherService) isFileTracked(file string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, exists := s.lock[file]
	return exists
}

// getFileStatus returns the current status of the file.
func (s *FileWatcherService) getFileStatus(file string) FileStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lock[file].Status
}

// setFileStatus updates the status and read lines number for the given file.
func (s *FileWatcherService) setFileStatus(file string, status FileStatus, readLinesNumber int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if currentStatus := s.lock[file].Status; currentStatus == Completed && status != Completed {
		fmt.Printf("Cannot change status from completed to %s for file %s\n", status, file)
		return
	}
	s.lock[file] = FileLock{Status: status, ReadLinesNumber: readLinesNumber}
	fmt.Printf("File %s status updated to %s\n", file, status)
}

// pickNextFile selects the next file to process in a round-robin manner.
func (s *FileWatcherService) pickNextFile() string {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.filesQueue) == 0 {
		return ""
	}

	for i := 0; i < len(s.filesQueue); i++ {
		index := (s.currentFileIndex + i) % len(s.filesQueue)
		currentFile := s.filesQueue[index]
		if s.lock[currentFile].Status != Completed {
			s.currentFileIndex = (index + 1) % len(s.filesQueue)
			return currentFile
		}
	}

	return ""
}

// processFiles processes files using workers, ensuring no more than maxThreads run concurrently.
func (s *FileWatcherService) processFiles() {
	if s.activeWorkers >= s.maxThreads {
		return
	}

	file := s.pickNextFile()
	if file != "" {
		s.setFileStatus(file, Reading, s.lock[file].ReadLinesNumber)
		s.activeWorkers++
		worker := NewWorker(file, s)
		go worker.startProcessing()
	}
}

// Worker processes a file and its chunks.
type Worker struct {
	file    string
	service *FileWatcherService
}

// NewWorker creates a new Worker instance.
func NewWorker(file string, service *FileWatcherService) *Worker {
	return &Worker{file: file, service: service}
}

// startProcessing starts the worker's task of reading and processing a file.
func (w *Worker) startProcessing() {
	w.readFile()
}

// readFile reads and processes the file in chunks.
func (w *Worker) readFile() {
	fmt.Printf("Start reading file %s\n", w.file)
	filePath := filepath.Join(DATA_FILES_DIR, w.file)

	file, err := os.Open(filePath)
	if err != nil {
		fmt.Printf("Error opening file %s: %v\n", w.file, err)
		w.service.activeWorkers--
		return
	}
	defer file.Close()

	reader := csv.NewReader(file)
	var results []map[string]string
	readLines := w.service.lock[w.file].ReadLinesNumber
	tempReadLines := readLines

	for {
		record, err := reader.Read()
		if err != nil {
			// End of file or error
			break
		}

		// Skip empty lines (check if all columns in the record are empty)
		if isEmptyLine(record) {
			continue
		}

		// If there are remaining lines to skip, decrement the counter
		if readLines > 0 {
			readLines--
			continue
		}

		// Process the record if it's not empty and within the chunk limit
		if len(results) < MAX_CHUNK_SIZE {
			results = append(results, map[string]string{"id": record[0], "name": record[1]})
			tempReadLines++
		} else {
			break
		}
	}

	// Append processed data to result file
	w.appendToResultFile(results)

	// Update the file status based on the processing outcome
	if len(results) < MAX_CHUNK_SIZE {
		w.service.setFileStatus(w.file, Completed, tempReadLines)
		fmt.Printf("File %s has been completed.\n", w.file)
	} else {
		w.service.setFileStatus(w.file, NotRead, tempReadLines)
		fmt.Printf("File %s is set to 'not_read' for further processing.\n", w.file)
	}

	// Decrease the worker count once done
	w.service.activeWorkers--
}

// isEmptyLine checks if the CSV record is empty.
func isEmptyLine(record []string) bool {
	// Check if all columns are empty (empty strings or no values)
	for _, field := range record {
		if field != "" {
			return false // Found a non-empty field, return false
		}
	}
	return true // All fields are empty
}

// appendToResultFile appends the processed data to the result file.
func (w *Worker) appendToResultFile(chunk []map[string]string) {
	file, err := os.OpenFile(RESULT_FILE_PATH, os.O_APPEND|os.O_WRONLY|os.O_CREATE, os.ModePerm)
	if err != nil {
		fmt.Printf("Error opening result file: %v\n", err)
		return
	}
	defer file.Close()

	for _, row := range chunk {
		_, err := fmt.Fprintf(file, "%s,%s\n", row["id"], row["name"])
		if err != nil {
			fmt.Printf("Error writing to result file: %v\n", err)
		}
	}

	fmt.Printf("Appended chunk to %s\n", RESULT_FILE_PATH)
}

func main() {
	// Instantiate the file watcher service
	service := NewFileWatcherService(
		DATA_FILES_DIR,
		MAX_THREADS_COUNT,
		INTERVAL_MS_CHECK_FILES,
		INTERVAL_MS_PROCESS_FILES,
	)

	// Start the service to watch files and process them
	service.StartWatching()

	// Block forever (or replace with a more advanced termination mechanism)
	select {}
}
