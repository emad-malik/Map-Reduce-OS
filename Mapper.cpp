#include <iostream>
#include <sstream>
#include <vector>
#include <string>
#include <utility>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <pthread.h>

using namespace std;
using KeyCountPair= std::pair<std::string, int>;

pthread_mutex_t safe_lock= PTHREAD_MUTEX_INITIALIZER;

// struct for thread so each thread writes a unique word to the pipe
struct ThreadData 
{
    vector<string> words;  
};

// thread function to process a chunk of words
void* process_input_chunk(void* arg) 
{
    ThreadData* data= (ThreadData*) arg;
    vector<KeyCountPair> word_count;

    for (const string& word : data->words) 
    {
        word_count.emplace_back(word, 1);
    }

    // write the output of mapper phase to the pipe
    int pipe_fd= open("MapReduce", O_WRONLY);
    if (pipe_fd < 0) 
    {
        perror("Error opening Pipe for writing!");
        pthread_exit(NULL);
    }

    // lock mutex before writing to pipe
    pthread_mutex_lock(&safe_lock);

    for (const auto& pair : word_count) 
    {
        string output= pair.first + " " + to_string(pair.second) + "\n";
        write(pipe_fd, output.c_str(), output.size());
        cout<<"Data written to pipe MapReduce: "<<output;  
    }

    pthread_mutex_unlock(&safe_lock);
    close(pipe_fd);

    pthread_exit(NULL);
}

// Mapper function
void mapper(const string& user_input) 
{
    mkfifo("MapReduce", 0666);

    vector<pthread_t> threads; // vector to hold threads
    vector<string> words;
    istringstream stream(user_input);
    string word;

    // input tokenization
    while (stream >> word) 
    {
        words.push_back(word);
    }

    int chunk_size= 2;
    int num_chunks= (words.size() + chunk_size - 1) / chunk_size;

    // process the chunk of words 
    for (int i= 0; i < num_chunks; i++) 
    {
        ThreadData* data= new ThreadData;
        int start_index= i * chunk_size;
        int end_index= min((i + 1) * chunk_size, (int)words.size());
        for (int j= start_index; j < end_index; ++j) 
        {
            data->words.push_back(words[j]);
        }
        
        pthread_t thread;
        pthread_create(&thread, NULL, process_input_chunk, (void*)data);
        threads.push_back(thread);
    }

    // wait for threads to finish
    for (pthread_t& thread : threads) 
    {
        pthread_join(thread, NULL);
    }

    cout<<"Mapper: Key-Value pairs sent to Reducer!\n";
}

int main() 
{
    pthread_mutex_init(&safe_lock, NULL);

    cout<<"Enter the input text: ";
    string user_input;
    getline(cin, user_input);

    mapper(user_input);

    pthread_mutex_destroy(&safe_lock);

    pthread_exit(NULL);
}
