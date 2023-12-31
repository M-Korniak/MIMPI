/**
 * This file is for implementation of MIMPI library.
 * */
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <math.h>

#include <pthread.h>
#include <string.h>
#include <errno.h>


#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"

#define BUFF_SIZE 512
#define FINISH_TAG -1
#define BARRIER_TAG -2
#define BROKEN_BARRIER_TAG -3

typedef struct message_t {
    int process_number;
    int tag;
    int count;
    int read_bytes;
    void* data;
    struct message_t* next;
    struct message_t* prev;
} message;

typedef struct list_t {
    message* head;
    message* tail;
} list;

static int world_size;
static int world_rank;
static int read_pipe_dsc;
static int finished_processes_count = 0;
static int broken_barrier = 0;

static int* write_pipe_dsc;
static int* finished_processes;
static void* send_buffer;
static void* receive_buffer;
static list* messages;
static message** messages_to_process;
static pthread_t reading_thread_id;
static pthread_mutex_t mutex;
static pthread_cond_t main_waiting;



void list_init(list* l) {
    l->head = NULL;
    l->tail = NULL;
}

void list_append(list* l, message* m) {
    if (l->head == NULL) {
        l->head = m;
        l->tail = m;
        m->next = NULL;
        m->prev = NULL;
    }
    else {
        l->tail->next = m;
        m->prev = l->tail;
        m->next = NULL;
        l->tail = m;
    }
}

void list_remove(list* l, message* m) {
    if (m->prev == NULL) {
        l->head = m->next;
    }
    else {
        m->prev->next = m->next;
    }
    if (m->next == NULL) {
        l->tail = m->prev;
    }
    else {
        m->next->prev = m->prev;
    }
}

void list_free(list* l) {
    message* m = l->head;
    while (m != NULL) {
        message* next = m->next;
        free(m->data);
        free(m);
        m = next;
    }
    free(l);

}

static void* reading_thread(void* data) {
    while(true) {
        // Process number
        int ret = chrecv(read_pipe_dsc, receive_buffer, sizeof(int));
        if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
            break;
        }
        ASSERT_SYS_OK(ret);
        int process_number = *(int*)receive_buffer;
        // Tag
        ret = chrecv(read_pipe_dsc, receive_buffer, sizeof(int));
        if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
            break;
        }
        ASSERT_SYS_OK(ret);
        int tag = *(int*)receive_buffer;
        if (tag == FINISH_TAG) {
            // Mutex lock
            ASSERT_SYS_OK(pthread_mutex_lock(&mutex));
            finished_processes[process_number] = 1;
            finished_processes_count++;
    
            // Signal main thread
            ASSERT_SYS_OK(pthread_cond_signal(&main_waiting));

            // Mutex unlock
            ASSERT_SYS_OK(pthread_mutex_unlock(&mutex));
            continue;
        }
        // Count
        ret = chrecv(read_pipe_dsc, receive_buffer, sizeof(int));
        if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
            break;
        }
        ASSERT_SYS_OK(ret);
        int count = *(int*)receive_buffer;
        if (tag == BARRIER_TAG) {
            // Mutex lock
            ASSERT_SYS_OK(pthread_mutex_lock(&mutex));
        

            // Create message
            message* m = malloc(sizeof(message));
            assert(m != NULL);
            m->process_number = process_number;
            m->tag = tag;
            m->count = 0;
            m->read_bytes = 0;
            m->data = NULL;
            m->next = NULL;
            m->prev = NULL;
            list_append(messages, m);

            // Signal main thread
            ASSERT_SYS_OK(pthread_cond_signal(&main_waiting));
            // Mutex unlock
            ASSERT_SYS_OK(pthread_mutex_unlock(&mutex));
            continue;
        }
        else if (tag == BROKEN_BARRIER_TAG) {
            // Mutex lock
            ASSERT_SYS_OK(pthread_mutex_lock(&mutex));
            broken_barrier = 1;
            // Signal main thread
            ASSERT_SYS_OK(pthread_cond_signal(&main_waiting));
            // Mutex unlock
            ASSERT_SYS_OK(pthread_mutex_unlock(&mutex));
            continue;
        }
        // Bytes to read
        ret = chrecv(read_pipe_dsc, receive_buffer, sizeof(int));
        if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
            break;
        }
        ASSERT_SYS_OK(ret);
        int to_read = *(int*)receive_buffer;
        // Read data
        ret = chrecv(read_pipe_dsc, receive_buffer, to_read);
        if (ret == -1 && errno == EBADF) {
            break;
        }
        ASSERT_SYS_OK(ret);
        // Mutex lock
        ASSERT_SYS_OK(pthread_mutex_lock(&mutex));
        // Create message
        if (messages_to_process[process_number] == NULL) {
            messages_to_process[process_number] = malloc(sizeof(message));
            assert(messages_to_process[process_number] != NULL);
            messages_to_process[process_number]->process_number = process_number;
            messages_to_process[process_number]->tag = tag;
            messages_to_process[process_number]->count = count;
            messages_to_process[process_number]->read_bytes = to_read;
            messages_to_process[process_number]->data = malloc(count);
            assert(messages_to_process[process_number]->data != NULL);
            memcpy(messages_to_process[process_number]->data, receive_buffer, to_read);
            messages_to_process[process_number]->next = NULL;
            messages_to_process[process_number]->prev = NULL;
        }
        else {
            message* m = messages_to_process[process_number];
            memcpy(m->data + m->read_bytes, receive_buffer, to_read);
            m->read_bytes += to_read;
        }
        // Check if message is complete
        if (messages_to_process[process_number]->read_bytes == messages_to_process[process_number]->count) {
            list_append(messages, messages_to_process[process_number]);
            messages_to_process[process_number] = NULL;
            ASSERT_SYS_OK(pthread_cond_signal(&main_waiting));
        }
        // Mutex unlock
        ASSERT_SYS_OK(pthread_mutex_unlock(&mutex));



    }
    return NULL;
}

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    // Get world size and rank from environment variables
    char* env_size = getenv("MIMPI_WORLD_SIZE");
    assert(env_size != NULL);
    world_size = atoi(env_size);
    char* env_rank = getenv("MIMPI_WORLD_RANK");
    assert(env_rank != NULL);
    world_rank = atoi(env_rank);

    // Alloc memory for pipes descriptors and finished processes
    read_pipe_dsc = 20 + 2 * world_rank;
    write_pipe_dsc = malloc(sizeof(int) * world_size);
    assert(write_pipe_dsc != NULL);
    finished_processes = calloc(world_size, sizeof(int));
    assert(finished_processes != NULL);

    for (int i = 0; i < world_size; i++) {
        write_pipe_dsc[i] = 21 + 2 * i;
    }

    // Close reading pipes
    for (int i = 20; i < 20 + 2 * world_size; i += 2) {
        if (i != 20 + 2 * world_rank) {
            ASSERT_SYS_OK(close(i));
        }
    }

    // Initialize mutex and condition variables
    ASSERT_SYS_OK(pthread_mutex_init(&mutex, NULL));
    ASSERT_SYS_OK(pthread_cond_init(&main_waiting, NULL));

    // Aloc memory for buffers
    send_buffer = malloc(BUFF_SIZE);
    assert(send_buffer != NULL);
    receive_buffer = malloc(BUFF_SIZE);
    assert(receive_buffer != NULL);

    // Initialize list of messages
    messages = malloc(sizeof(list));
    assert(messages != NULL);
    list_init(messages);

    // Initialize list of messages to process for each process
    messages_to_process = malloc(sizeof(message*) * world_size);
    assert(messages_to_process != NULL);
    for (int i = 0; i < world_size; i++) {
        messages_to_process[i] = NULL;
    }
    

    // Create reading thread
    ASSERT_ZERO(pthread_create(&reading_thread_id, NULL, reading_thread, NULL));

    
}

void* send_finish(void* arg) {
    int destination = *(int*)arg;
    int left = 2 * destination + 1;
    int right = 2 * destination + 2;
    pthread_t right_thread_id;
    // Create local buffer
    void* local_buffer = malloc(2 * sizeof(int));
    if (destination != world_rank && finished_processes[destination] == 0) {
            *(int*)local_buffer = world_rank;
            *(int*)(local_buffer + sizeof(int)) = FINISH_TAG;
            printf("Process %d sending finish to %d\n", world_rank, destination);
            int ret = chsend(write_pipe_dsc[destination], local_buffer, 2 * sizeof(int));
            if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
                free(local_buffer);
                return NULL;
            }
            ASSERT_SYS_OK(ret);
    }
    if (right < world_size) {
        // Create thread for right child
        ASSERT_ZERO(pthread_create(&right_thread_id, NULL, send_finish, &right));
    }
    if (left < world_size) {
        send_finish(&left);
    }
    if (right < world_size) {
        ASSERT_ZERO(pthread_join(right_thread_id, NULL));
    }
    free(local_buffer);
    return NULL;
}

void* create_send(void* arg) {
    int destination = *(int*)arg;
    int left = 2 * destination + 1;
    int right = 2 * destination + 2;
    pthread_t left_thread_id;
    pthread_t right_thread_id;
    if (left < world_size) {
        // Create thread for left child
        printf("Process %d creating thread for %d\n", world_rank, left);
        ASSERT_ZERO(pthread_create(&left_thread_id, NULL, create_send, &left));
    }
    if (right < world_size) {
        // Create thread for right child
        printf("Process %d creating thread for %d\n", world_rank, right);
        ASSERT_ZERO(pthread_create(&right_thread_id, NULL, create_send, &right));
    }
    void* local_buffer = malloc(2 * sizeof(int));
    if (destination != world_rank && finished_processes[destination] == 0) {
            *(int*)local_buffer = world_rank;
            *(int*)(local_buffer + sizeof(int)) = FINISH_TAG;
            printf("Process %d sending finish to %d\n", world_rank, destination);
            int ret = chsend(write_pipe_dsc[destination], local_buffer, 2 * sizeof(int));
            if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
                free(local_buffer);
                return NULL;
            }
            ASSERT_SYS_OK(ret);
    }
    if (left < world_size) {
        printf("Process %d waiting for %d\n", world_rank, left);
        ASSERT_ZERO(pthread_join(left_thread_id, NULL));
    }
    if (right < world_size) {
        printf("Process %d waiting for %d\n", world_rank, right);
        ASSERT_ZERO(pthread_join(right_thread_id, NULL));
    }
    free(local_buffer);
    printf("Process %d with destiantion %d finished\n", world_rank, destination);
    return NULL;
}
void MIMPI_Finalize() {
    // Close reading pipe
    ASSERT_SYS_OK(close(read_pipe_dsc));

    int arg = 0;
    create_send(&arg);
    
    // pthread_t* threads = malloc(world_rank * sizeof(pthread_t));

    // int* args = malloc(world_rank * sizeof(int));

    // // Create threads
    // for (int i = 0; i < world_rank; i++) {
    //     args[i] = i;
    //     ASSERT_ZERO(pthread_create(&threads[i], NULL, send_signal, &args[i]));
    // }

    // // Wait for threads to finish
    // for (int i = 0; i < world_rank; i++) {
    //     ASSERT_ZERO(pthread_join(threads[i], NULL));    
    // }

    // free(threads);
    // free(args);



    // // Send finishing signal to all processes
    // for (int i = 0; i < world_size; i++) {
    //     if (i != world_rank) {
    //         *(int*)send_buffer = world_rank;
    //         *(int*)(send_buffer + sizeof(int)) = FINISH_TAG;
    //         int ret = chsend(write_pipe_dsc[i], send_buffer, 2 * sizeof(int));
    //         if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
    //             continue;
    //         }
    //         ASSERT_SYS_OK(ret);
    //     }
    // }

    // Send stopping signal to thread and wait for it to finish
    ASSERT_ZERO(pthread_join(reading_thread_id, NULL));

    // Close mutex and condition variables
    ASSERT_SYS_OK(pthread_mutex_destroy(&mutex));
    ASSERT_SYS_OK(pthread_cond_destroy(&main_waiting));

    // Close pipes
    for (int i = 0; i < world_size; i++) {
        ASSERT_SYS_OK(close(write_pipe_dsc[i]));
    }
    
    // Free memory
    free(write_pipe_dsc);
    free(finished_processes);
    free(send_buffer);
    free(receive_buffer);
    for (int i = 0; i < world_size; i++) {
        if (messages_to_process[i] != NULL) {
            free(messages_to_process[i]->data);
            free(messages_to_process[i]);
        }
    }
    free(messages_to_process);
    list_free(messages);

    printf("Process %d finished\n", world_rank);
    channels_finalize();
}

int MIMPI_World_size() {
    return world_size;
}

int MIMPI_World_rank() {
    return world_rank;
}

MIMPI_Retcode MIMPI_Send(
    void const *data,
    int count,
    int destination,
    int tag
) {
    if (destination < 0 || destination >= world_size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    if (destination == world_rank && (tag == BARRIER_TAG || tag == BROKEN_BARRIER_TAG)) {
        return MIMPI_SUCCESS;
    }
    if (destination == world_rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }

    void const *data_index = data;
    *(int*)send_buffer = world_rank;
    *(int*)(send_buffer + sizeof(int)) = tag;
    *(int*)(send_buffer + 2 * sizeof(int)) = count;

    if (count == 0) {
        int ret = chsend(write_pipe_dsc[destination], send_buffer, 3 * sizeof(int));
        if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
    }

    while (count > 0) {
        int to_write = count > BUFF_SIZE - 4 * sizeof(int) ? BUFF_SIZE - 4 * sizeof(int) : count;
        *(int*)(send_buffer + 3 * sizeof(int)) = to_write;
        memcpy(send_buffer + 4 * sizeof(int), data_index, to_write);
        int ret = chsend(write_pipe_dsc[destination], send_buffer, to_write + 4 * sizeof(int));
        if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        ASSERT_SYS_OK(ret);
        data_index += to_write;
        count -= to_write;
    }
    
    return MIMPI_SUCCESS;
}

static int look_for_message(int source, int tag, int count) {
    message* m = messages->head;
    while (m != NULL) {
        if (m->process_number == source && (m->tag == tag || (tag == 0 && m->tag > 0)) && m->count == count) {
            return 1;
        }
        m = m->next;
    }
    return 0;
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    if (source < 0 || source >= world_size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    if (source == world_rank && (tag == BARRIER_TAG || tag == BROKEN_BARRIER_TAG)) {
        return MIMPI_SUCCESS;
    }
    if (source == world_rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }

    // Mutex lock
    ASSERT_SYS_OK(pthread_mutex_lock(&mutex));
    
    while (!look_for_message(source, tag, count) && finished_processes[source] == 0 
            && broken_barrier == 0) {
        ASSERT_SYS_OK(pthread_cond_wait(&main_waiting, &mutex));
    }


    // Look for message
    message* m = messages->head;
    while (m != NULL) {
        if (m->process_number == source && (m->tag == tag || (tag == 0 && m->tag > 0)) && m->count == count) {
            if (data != NULL)
                memcpy(data, m->data, count);
            list_remove(messages, m);
            free(m->data);
            free(m);
            ASSERT_SYS_OK(pthread_mutex_unlock(&mutex));
            return MIMPI_SUCCESS;
        }
        m = m->next;
    }

    // Mutex unlock
    ASSERT_SYS_OK(pthread_mutex_unlock(&mutex));
    return MIMPI_ERROR_REMOTE_FINISHED;
}


MIMPI_Retcode MIMPI_Barrier() {
    int value_to_return = MIMPI_SUCCESS;
    int ret = 0;
    int local_finished_processes[16];
    for (int i = 0; i < log2(world_size); i++) {
        // TODO avoid sending to yourself
        int partner = (world_rank + (1 << i)) % world_size;
    
        if (value_to_return == MIMPI_SUCCESS)
            ret = MIMPI_Send(NULL, 0, partner, BARRIER_TAG);
        else if (value_to_return == MIMPI_ERROR_REMOTE_FINISHED) 
            MIMPI_Send(NULL, 0, partner, BROKEN_BARRIER_TAG);
        if (ret == MIMPI_ERROR_REMOTE_FINISHED) 
            value_to_return = MIMPI_ERROR_REMOTE_FINISHED;
        int previous = (world_rank - (1 << i) + world_size) % world_size;
        if (value_to_return == MIMPI_SUCCESS) {
            ret = MIMPI_Recv(NULL, 0, previous, BARRIER_TAG);
            //local_finished_processes[previous] = 1;
        }
        if (ret == MIMPI_ERROR_REMOTE_FINISHED) 
            value_to_return = MIMPI_ERROR_REMOTE_FINISHED;
    }
    
    return value_to_return;
}

MIMPI_Retcode MIMPI_Bcast(
    void *data,
    int count,
    int root
) {
    TODO
}

MIMPI_Retcode MIMPI_Reduce(
    void const *send_data,
    void *recv_data,
    int count,
    MIMPI_Op op,
    int root
) {
    TODO
}