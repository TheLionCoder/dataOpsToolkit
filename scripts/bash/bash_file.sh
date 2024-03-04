#!/bin/bash

# Output file nane for hashes
output_file_name="01-hashes.txt"

# Uses openssl to hash the file faster
hash_command="openssl dgst -sha256 -r"

hash_file(){
    local_file="$1"
    # Check if the file exists
    if [ -f "$local_file" ] && [ "$local_file" != "$output_file_name"]; then
    # Calculate the sha-256
    hash=$($hash_command "$local_file" | cut -d ' ' -f1)
    #append the file name and hash to the output file
    echo "$loca_file $hash" >> "$output_file_name"
    else
    echo "File $local_file does not exist"
    fi
}

# Main
# Check if a file path or extension are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 [directory] - [extension]"
    exit 1
fi

directory="$1"
extension="$2"

# call the function
for file in "$directory"/*."$extension"; do
    hash_file "$file"
    done

echo "Hashing completed and succesfully saved to $output_file_name"
