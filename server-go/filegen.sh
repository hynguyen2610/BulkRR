#!/bin/bash

# Check if the correct number of arguments are passed
if [ $# -lt 2 ] || [ $# -gt 3 ]; then
  echo "Usage: $0 <num_files> <num_lines_per_file> [filename_prefix]"
  exit 1
fi

# Get the arguments
num_files=$1
num_lines=$2
filename_prefix=${3:-"filename_"}  # Default prefix is "filename_" if not provided
server_url="localhost:9500/upload"

# Clean up any existing CSV files
rm -f ${filename_prefix}*.csv

# Print the number of files and lines to debug
echo "Generating $num_files files, each with $num_lines lines, using prefix '$filename_prefix'."

# Step 1: Generate the CSV files
generate_csv_files() {
  local file_name="$1"
  local lines="$2"

  # Generate lines for the CSV file
  for i in $(seq 1 "$lines"); do
    name="Name$i"
    echo "$file_name.$i, $name" >>"./data/$file_name"
  done

  echo "$file_name generated."
}

# Step 2: Submit the files
submit_csv_file() {
  local file_name="$1"

  # Submit the file to the queue via POST request with the correct format
  curl --location "$server_url" \
    --form "files=@\"$file_name\""  # Adjust the file path
  echo "Submitted $file_name"
}

# Step 1: Generate all the files (in parallel)
echo "Generating CSV files..."
for i in $(seq 1 "$num_files"); do
  file_name="${filename_prefix}$i.csv"
  echo "Generating file: $file_name"
  generate_csv_files "$file_name" "$num_lines" & # Background process
done

# Wait for all file generation processes to complete
wait
echo "All files generated."

# Step 2: Submit all files in parallel using background processes
echo "Submitting files in parallel..."
for file in ${filename_prefix}*.csv; do
  submit_csv_file "$file" & # Background process
done

# Wait for all submission processes to complete
wait
echo "All files submitted."
