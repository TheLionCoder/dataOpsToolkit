#!/bin/bash


# Uses openssl to hash the file faster
hash_command="openssl dgst -sha256 -r"

hash_file(){
    local_file="$1"
    file_name=$(basename "$local_file")
    # Check if the file exists
    if [ -f "$local_file" ] && [ "$local_file" != "$output_file_name" ]; then
    # Calculate the sha-256
    hash=$($hash_command "$local_file" | cut -d ' ' -f1)
    #append the file name and hash to the output file
    echo "$file_name $hash" >> "$output_file_name"
    else
    echo "File $file_name does not exist"
    fi
}

# Export the function so it's available in subshells
export -f hash_file

# Main
# Check if a file path or extension are provided
if [ "$#" -ne 2 ]; then
    echo "Usage: $0 [directory] - [extension]"
    exit 1
fi

directory="$1"
extension="$2"

# Output file nane for hashes
output_file_name="$directory/01-hashes.txt"

# call the function
for file in "$directory"/*."$extension"; do
    hash_file "$file"
done

echo "Hashing completed and succesfully saved to $output_file_name"
