import fs from 'fs';
import path from 'path';
import { parse } from '@fast-csv/parse';
import { EventEmitter } from 'events';

// Define types for the lock state and file queue status
type FileStatus = 'not_read' | 'reading' | 'completed';
interface FileLock {
  [key: string]: {
    status: FileStatus;
    readLinesNumber: number; // Track the number of lines read
  };
}

const DATA_FILES_DIR = './data';
const RESULT_FILE_PATH = './result.txt'; // Path to the result.txt
const MAX_THREADS_COUNT = 4;
const MAX_CHUNK_SIZE = 3;
const INTERVAL_MS_CHECK_FILES = 1000; // Check new files every second
const INTERVAL_MS_PROCESS_FILES = 1000; // Process files every second

// Singleton FileWatcherService
class FileWatcherService extends EventEmitter {
  private static instance: FileWatcherService | null = null;
  public activeWorkers: number;
  private filesQueue: string[];
  private lock: FileLock;
  private currentFileIndex: number; // Track the current index for round-robin
  private watching: boolean;
  private maxThreads: number;
  private intervalMsCheckFiles: number;
  private intervalMsProcessFiles: number;
  private folderPath: string;

  // Private constructor to enforce singleton pattern
  private constructor(
    folderPath: string,
    maxThreads: number,
    intervalMsCheckFiles: number,
    intervalMsProcessFiles: number
  ) {
    super();
    this.folderPath = folderPath;
    this.maxThreads = maxThreads;
    this.intervalMsCheckFiles = intervalMsCheckFiles;
    this.intervalMsProcessFiles = intervalMsProcessFiles;
    this.activeWorkers = 0; // Number of active workers
    this.filesQueue = []; // Queue of files with status
    this.lock = {}; // Locking mechanism to track the status of each file
    this.currentFileIndex = 0; // Start with the first file in the queue
    this.watching = false; // To ensure we only start the interval once
  }

  // Singleton Instance
  public static getInstance(
    folderPath: string = DATA_FILES_DIR,
    maxThreads: number = MAX_THREADS_COUNT,
    intervalMsCheckFiles: number = INTERVAL_MS_CHECK_FILES,
    intervalMsProcessFiles: number = INTERVAL_MS_PROCESS_FILES
  ): FileWatcherService {
    if (!FileWatcherService.instance) {
      FileWatcherService.instance = new FileWatcherService(
        folderPath,
        maxThreads,
        intervalMsCheckFiles,
        intervalMsProcessFiles
      );
    }
    return FileWatcherService.instance;
  }

  // Start polling the directory for new files
  startWatching(): void {
    console.log('Start file watching');
    if (this.watching) return;

    this.watching = true;

    // Check for new files at the interval
    setInterval(() => {
      this.checkForNewFiles();
    }, this.intervalMsCheckFiles)

    // Start processing files at the interval
    setInterval(() => {
      this.processFiles();
    }, this.intervalMsProcessFiles);
  }

  // Check the directory for new CSV files and add them to the queue
  private checkForNewFiles(): void {
    fs.readdir(this.folderPath, (err, files) => {
      if (err) {
        console.error(`Error reading directory: ${err}`);
        return;
      }

      // Filter out only CSV files
      const newFiles = files.filter((file) => file.endsWith('.csv'));

      newFiles.forEach((file) => {
        if (!this.lock[file]) {
          // If the file is not being tracked, add it to the queue with status 'not_read' and readLinesNumber 0
          this.lock[file] = { status: 'not_read', readLinesNumber: 0 };
          this.filesQueue.push(file);
          console.log(
            `Added new file ${file} to the queue with status 'not_read'`
          );
        }
      });
    });
  }

  // Get file status
  public getFileStatus(file: string): FileStatus {
    return this.lock[file]?.status || 'not_read';
  }

  // Get the number of lines already read in the file
  public getFileStateReadLines(file: string): number {
    return this.lock[file]?.readLinesNumber || 0;
  }

  // Update file status and read lines number
  public setFileStatus(
    file: string,
    status: FileStatus,
    readLinesNumber: number
  ): void {
    const currentStatus = this.lock[file]?.status;

    if (currentStatus === 'completed' && status !== 'completed') {
      console.warn(`Cannot change status from completed to ${status} for file ${file}`);
      return;
    }

    this.lock[file] = { status, readLinesNumber };
    console.log(`File ${file} status updated to ${status}`);
  }

  // Pick the next file to process (round-robin)
  public pickNextFile(): string | null {
    if (this.filesQueue.length === 0) {
      return null;
    }

    // Find the next file that is not completed, and ensure we process them in order (round-robin)
    let file: string | null = null;

    for (let i = 0; i < this.filesQueue.length; i++) {
      const index = (this.currentFileIndex + i) % this.filesQueue.length;
      const currentFile = this.filesQueue[index];

      // Process the file if it's not completed
      if (this.lock[currentFile]?.status !== 'completed') {
        file = currentFile;
        this.currentFileIndex = (index + 1) % this.filesQueue.length; // Move to the next file in the queue
        break;
      }
    }

    return file;
  }

  // Process files in a round-robin manner (called by interval)
  private processFiles(): void {
    // Ensure no more than maxThreads are running at the same time
    if (this.activeWorkers >= this.maxThreads) return;

    // Get the next file to process using round-robin
    const file = this.pickNextFile();

    if (file) {
      // Mark file as 'reading' to avoid other workers processing it
      this.setFileStatus(file, 'reading', this.getFileStateReadLines(file));

      this.activeWorkers++;

      // Create a worker for the file and process it
      const worker = new Worker(file, this);
      worker.startProcessing();
    }
  }
}

// Worker Class that handles file reading and chunk processing
class Worker {
  private file: string;
  private service: FileWatcherService;

  constructor(file: string, service: FileWatcherService) {
    this.file = file;
    this.service = service;
  }

  // Start processing the file
  public startProcessing(): void {
    this.readFile();
  }

  // Read a single chunk from the file and process it synchronously
  private readFile(): void {
    console.log(`Start reading file ${this.file}`);
    const filePath = path.join(DATA_FILES_DIR, this.file);

    const results: { id: string, name: string }[] = [];
    let readLines = this.service.getFileStateReadLines(this.file); // Get how many lines were already read
    let currentChunkSize = 0;

    const stream = fs
      .createReadStream(filePath)
      .pipe(parse({ headers: false })) // Skip the lines that were already processed
      .on('data', (row) => {
        if (readLines > 0) {
          // Skip lines until we reach the desired read position
          readLines--;
          return;
        }

        // Process the row if we have not yet reached the chunk size
        if (currentChunkSize < MAX_CHUNK_SIZE && row && row.length > 0) {
          currentChunkSize++;
          const [id, name] = row;
          results.push({ id, name });
        }
      })
      .on('end', () => {
        console.log(`Finished processing chunk of file ${this.file}`);

        // Append chunk to result.txt
        this.appendToResultFile(results);

        // After processing one chunk, determine if the file is complete
        if (results.length < MAX_CHUNK_SIZE) {
          // If the chunk size is less than MAX_CHUNK_SIZE, it means the file is completed
          this.service.setFileStatus(
            this.file,
            'completed',
            this.service.getFileStateReadLines(this.file) + results.length
          );
          console.log(`File ${this.file} has been completed.`);
        } else {
          // File is not completed yet, we set it back to 'not_read' for further processing
          this.service.setFileStatus(
            this.file,
            'not_read',
            this.service.getFileStateReadLines(this.file) + results.length
          );
          console.log(`File ${this.file} is set to 'not_read' for further processing.`);
        }

        // Emit the file completion event
        this.service.emit('fileCompleted', this.file);

        // Decrease the active worker count and continue processing other files
        this.service.activeWorkers--;
      })
      .on('error', (err) => {
        console.error(`Error reading file ${this.file}: ${err}`);
        this.service.activeWorkers--;
      });
  }

  // Append processed chunk to result.txt
  private appendToResultFile(chunk: { id: string, name: string }[]): void {
    fs.appendFileSync(RESULT_FILE_PATH, `Append a chunk of ${chunk.length} lines\n`);
    const chunkData = chunk.map(row => `${row.id},${row.name}`).join('\n');
    try {
      fs.appendFileSync(RESULT_FILE_PATH, chunkData + '\n');
      console.log(`Appended chunk to ${RESULT_FILE_PATH}`);
    } catch (err) {
      console.error(`Error appending to result.txt: ${err}`);
    }
  }
}

// Instantiate the singleton service using defined constants
const fileWatcherService = FileWatcherService.getInstance(
  DATA_FILES_DIR,
  MAX_THREADS_COUNT,
  INTERVAL_MS_CHECK_FILES,
  INTERVAL_MS_PROCESS_FILES
);

// Event listener for file completion
fileWatcherService.on('fileCompleted', (file) => {
  console.log(`File ${file} has been completed.`);
});

// Start watching the folder for new files
fileWatcherService.startWatching();

export { FileWatcherService, Worker };
