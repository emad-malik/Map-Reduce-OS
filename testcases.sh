#!/bin/bash

# File names for executables
MAPPER_EXEC="./mapper"
REDUCER_EXEC="./reducer"

# Named pipe
PIPE_NAME="MeraPyaraMapReducePipe"

# Test cases: an associative array of input and descriptions
declare -A TEST_CASES
TEST_CASES=(
    ["Basic Unique Words"]="umema emad haider"
    ["Repeated Words"]="map reduce map example reduce map"
    ["Empty Input"]=""
    ["Single Word"]="meow"
    ["All Identical Words"]="meow meow meow meow meow meow"
    ["Mixed Case Words"]="Word word WORD wOrD"
    ["Punctuation"]="hello, world! hello world."
    ["Large Input Repeated"]="$(printf 'fast nuces %.0s' {1..1000})"
    ["Large Input Unique"]="$(seq -f 'word%g' 1 1000 | tr '\n' ' ')"
)

# Ensure the named pipe exists
if [[ ! -p $PIPE_NAME ]]; then
    mkfifo $PIPE_NAME
fi

# Function to clean up the pipe on exit
cleanup() {
    rm -f $PIPE_NAME
    echo "Named pipe removed."
}

trap cleanup EXIT

# Run each test case
for description in "${!TEST_CASES[@]}"; do
    echo "========================================="
    echo "Test Case: $description"
    INPUT_STRING="${TEST_CASES[$description]}"
    echo "Input: \"$INPUT_STRING\""

    # Start the Reducer in the background
    $REDUCER_EXEC > "output_$description.txt" &

    # Run the Mapper with the current test case
    echo "$INPUT_STRING" | $MAPPER_EXEC

    # Wait for the Reducer to finish
    wait

    # Display the output
    echo "Output for $description:"
    cat "output_$description.txt"
    echo "========================================="
done

