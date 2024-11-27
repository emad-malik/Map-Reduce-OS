#include <iostream>
#include <sstream>
#include <map>
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
using namespace std;

void reducer()
{
    int pipe_fd = open("MapReduce", O_RDONLY);
    if (pipe_fd < 0) 
    {
        perror("Error opening MapPipe for reading!");
        return;
    }

    char buffer[1024];
    string data;
    ssize_t bytes_read;

    // Read data from the pipe
    while ((bytes_read = read(pipe_fd, buffer, sizeof(buffer) - 1)) > 0) 
    {
        buffer[bytes_read] = '\0';
        data += buffer;  // Append the data read from pipe to the string
    }
    close(pipe_fd);

    cout << "Reducer received raw data:\n" << data;


    // reducer phase logic -> collect data and aggregate it
   
}

int main()
{
    reducer();  // Call the reducer to process the data
    return 0;
}
