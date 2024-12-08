//OS PROJECT
//I222036 UMEMA ASHAR | I222072 EMAD MALIK | I221913 MUHAMMAD HAIDER

//Mapper.cpp

#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <utility>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>
#include <map>
#include <algorithm>  // For transform
#include <cctype>     // For ispunct()

using namespace std;
using KeyCountPair =  pair< string, int>;

pthread_mutex_t safeLock = PTHREAD_MUTEX_INITIALIZER;

struct ThreadData 
{
    vector<string> words;  
};

string cleanWord(const string& input) {
    string clean = input;
    // Convert to lowercase
    transform(clean.begin(), clean.end(), clean.begin(), ::tolower);
    // Remove punctuation
    clean.erase(remove_if(clean.begin(), clean.end(), ::ispunct), clean.end());
    return clean;
}

void* Splitting(void* arg) 
{
    ThreadData* data = (ThreadData*) arg;
    vector<KeyCountPair> word_count;

    for (const string& word : data->words) 
    {
        string cleanWordStr = cleanWord(word);  // Clean word (lowercase + no punctuation)
        if (!cleanWordStr.empty())  // Avoid adding empty words after cleaning
        {
            word_count.emplace_back(cleanWordStr, 1);
        }
    }

    pthread_exit((void*) new vector<KeyCountPair>(word_count)); 
}

void Shuffle(vector<KeyCountPair>& word_count) 
{
    map<string, vector<int>> grouped_data;
    
    for (const auto& pair : word_count) 
    {
        string cleanWordStr = cleanWord(pair.first);  // Clean word (lowercase + no punctuation)
        if (!cleanWordStr.empty())  // Avoid adding empty words after cleaning
        {
            grouped_data[cleanWordStr].push_back(pair.second);
        }
    }

    int pipe_fd = open("MeraPyaraMapReducePipe", O_WRONLY);
    if (pipe_fd < 0) 
    {
        perror("Error opening Pipe for writing!");
        return;
    }

    pthread_mutex_lock(&safeLock);

    for (const auto& entry : grouped_data) 
    {
        for (size_t i = 0; i < entry.second.size(); i++) 
        {
            string output = entry.first + " " + to_string(1) + "\n";
            write(pipe_fd, output.c_str(), output.size());
            /*cout << "Data Sent To Reducer: " << output;*/
            
        }
        /*cout << "Data Sent To Reducer" << endl;*/
    }

    pthread_mutex_unlock(&safeLock);
    close(pipe_fd);
}

void Mapper(const string& userInput) 
{
    mkfifo("MeraPyaraMapReducePipe", 0666);

    vector<pthread_t> threads;
    vector<KeyCountPair> allKeyValuePairs;
    
    cout << "Data Splitting Successful.\n";
    
    istringstream stream(userInput);
    string word;

    vector<string> words;
    while (stream >> word) 
    {
        words.push_back(word);
    }
    
    cout << "Data Mapping Successful.\n";

    for (size_t i = 0; i < words.size(); i++) 
    {
        ThreadData* data = new ThreadData{ {words[i]} };
        pthread_t thread;
        pthread_create(&thread, NULL, Splitting, (void*) data);
        threads.push_back(thread);
    }

    // Wait for all threads to finish and collect the results
    for (size_t i = 0; i < threads.size(); i++) 
    {
        void* return_value;
        pthread_join(threads[i], &return_value);
        vector<KeyCountPair>* word_count = (vector<KeyCountPair>*) return_value;
        allKeyValuePairs.insert(allKeyValuePairs.end(), word_count->begin(), word_count->end());
        delete word_count;
    }
    
    cout << "Data Shuffling Successful.\n\n";

    Shuffle(allKeyValuePairs);
    
    cout << "\nData Sent to Reducer Successfully.\n";
}

int main() 
{
    pthread_mutex_init(&safeLock, NULL);

    cout << "Enter the Input Text: ";
    string userInput;
    getline(cin, userInput);
    cout << endl;

    Mapper(userInput);

    pthread_mutex_destroy(&safeLock);

    pthread_exit(NULL);
}
