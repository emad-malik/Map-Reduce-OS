//OS PROJECT
//I222036 UMEMA ASHAR | I222072 EMAD MALIK | I221913 MUHAMMAD HAIDER

//Reducer.cpp

#include <iostream>
#include <sstream>
#include <map>
#include <string>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <pthread.h>

using namespace std;

void Reducer()
{
    int pipe_fd = open("MeraPyaraMapReducePipe", O_RDONLY);
    if (pipe_fd < 0) 
    {
        perror("Error opening MapReduce pipe for reading!");
        return;
    }

    map<string, int> word_count_map;
    char buffer[1024];
    string data;
    ssize_t bytes_read;

    while ((bytes_read = read(pipe_fd, buffer, sizeof(buffer) - 1)) > 0) 
    {
        buffer[bytes_read] = '\0';
        data += buffer;

        istringstream stream(data);
        string word;
        int count;

        while (stream >> word >> count) 
        {
            /*cout << "Data Received From Mapper: " << word << " " << count << endl;*/
            
            word_count_map[word] += count;
        }
        //cout << "Data Received From Mapper" << endl;

        data.clear();
    }
    
    cout << "\nData Received From Mapper Successfully.\n";
    close(pipe_fd);

    cout << "Data Reducing Successfully.\n";

    cout << "\n------ Final Result ------\n";
    for (const auto& entry : word_count_map) 
    {
        cout << entry.first << " " << entry.second << endl;
    }
}

int main()
{
    Reducer();
    return 0;
}
