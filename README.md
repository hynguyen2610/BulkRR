# Heavy Load Server

This service is designed to handle a heavy load of CSV files uploaded to it.
It processes the files in a round-robin manner, using a fixed number of worker
threads to read the files and append their contents to a single result file.

The service is intended to be used as a testing tool to simulate a heavy load
on a server. It can be used to test the performance and scalability of a
server under a heavy load of requests.

## How to Run

1. **Install Dependencies**: Ensure you have Node.js and npm installed. Navigate to the project directory and run:
   ```bash
   npm install
   ```

2. **Generate CSV Files**: Use the `filegen.sh` script to generate the CSV files. Specify the number of files and lines per file:
   ```bash
   ./server/filegen.sh <num_files> <num_lines_per_file> [filename_prefix]
   ```

3. **Start the Server**: Run the server using:
   ```bash
   npm start
   ```

4. **Submit Files**: The files will be automatically submitted to the server for processing.

5. **Check Results**: Once processing is complete, the results will be appended to `server/result.txt`.

6. **Clean Up**: To remove generated files and results, run:
   ```bash
   ./server/clean.sh
   ```

