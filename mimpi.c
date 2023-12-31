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
#define BCAST_TAG -1
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
static int finished_processes_count = 0;
static int broken_barrier = 0;
static int receive_from = -1;

static int* args;
static int* read_pipe_dsc;
static int* write_pipe_dsc;
static int* finished_processes;
static void* send_buffer;
static void** receive_buffer;
static list* messages;
static message** messages_to_process;
static pthread_t* reading_threads;
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

void actualize_finished_processes(int i) {
    // Mutex lock
    ASSERT_SYS_OK(pthread_mutex_lock(&mutex));
    finished_processes[i] = 1;
    finished_processes_count++;

    // Signal main thread
    ASSERT_SYS_OK(pthread_cond_signal(&main_waiting));

    // Mutex unlock
    ASSERT_SYS_OK(pthread_mutex_unlock(&mutex));
}
static void* reading_thread(void* data) {
    int source = *(int*)data;
    int read_pipe = read_pipe_dsc[source];
    void* buffer = receive_buffer[source];
    while(true) {
        // Tag
        int ret = chrecv(read_pipe, buffer, sizeof(int));
        if ((ret == -1 && (errno == EBADF || errno == EPIPE)) || ret == 0) {
            actualize_finished_processes(source);
            break;
        }
        ASSERT_SYS_OK(ret);
        int tag = *(int*)buffer;

        // Count
        ret = chrecv(read_pipe, buffer, sizeof(int));
        if ((ret == -1 && (errno == EBADF || errno == EPIPE)) || ret == 0) {
            actualize_finished_processes(source);
            break;
        }
        ASSERT_SYS_OK(ret);
        int count = *(int*)buffer;
        if (tag == BARRIER_TAG) {
            // Mutex lock
            ASSERT_SYS_OK(pthread_mutex_lock(&mutex));
        

            // Create message
            message* m = malloc(sizeof(message));
            assert(m != NULL);
            m->process_number = source;
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
        else if ((ret == -1 && (errno == EBADF || errno == EPIPE)) || ret == 0) {
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
        ret = chrecv(read_pipe, buffer, sizeof(int));
        if ((ret == -1 && (errno == EBADF || errno == EPIPE)) || ret == 0) {
            actualize_finished_processes(source);
            break;
        }
        ASSERT_SYS_OK(ret);
        int to_read = *(int*)buffer;
        // Read data
        ret = chrecv(read_pipe, buffer, to_read);
        if ((ret == -1 && (errno == EBADF || errno == EPIPE)) || ret == 0) {
            actualize_finished_processes(source);
            break;
        }
        ASSERT_SYS_OK(ret);
        // Mutex lock
        ASSERT_SYS_OK(pthread_mutex_lock(&mutex));
        // Create message
        if (messages_to_process[source] == NULL) {
            messages_to_process[source] = malloc(sizeof(message));
            assert(messages_to_process[source] != NULL);
            messages_to_process[source]->process_number = source;
            messages_to_process[source]->tag = tag;
            messages_to_process[source]->count = count;
            messages_to_process[source]->read_bytes = to_read;
            messages_to_process[source]->data = malloc(count);
            assert(messages_to_process[source]->data != NULL);
            memcpy(messages_to_process[source]->data, buffer, to_read);
            messages_to_process[source]->next = NULL;
            messages_to_process[source]->prev = NULL;
        }
        else {
            message* m = messages_to_process[source];
            memcpy(m->data + m->read_bytes, buffer, to_read);
            m->read_bytes += to_read;
        }
        // Check if message is complete
        if (messages_to_process[source]->read_bytes == messages_to_process[source]->count) {
            list_append(messages, messages_to_process[source]);
            messages_to_process[source] = NULL;
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
    read_pipe_dsc = malloc(sizeof(int) * world_size);
    assert(read_pipe_dsc != NULL);
    write_pipe_dsc = malloc(sizeof(int) * world_size);
    assert(write_pipe_dsc != NULL);
    finished_processes = calloc(world_size, sizeof(int));
    assert(finished_processes != NULL);

    for (int i = 0; i < world_size; i++) {
        for (int j = 0; j < world_size; j++) {
            if (i == j) {
                ASSERT_SYS_OK(close(20 + 2 * (i * world_size + j)));
                ASSERT_SYS_OK(close(21 + 2 * (i * world_size + j)));
            }
            else if (i == world_rank) {
                    read_pipe_dsc[j] = 20 + 2 * (i * world_size + j);
                    ASSERT_SYS_OK(close(21 + 2 * (i * world_size + j)));
            }
            else if (j == world_rank) {
                    ASSERT_SYS_OK(close(20 + 2 * (i * world_size + j)));
                    write_pipe_dsc[i] = 21 + 2 * (i * world_size + j);
            }
            else {
                ASSERT_SYS_OK(close(20 + 2 * (i * world_size + j)));
                ASSERT_SYS_OK(close(21 + 2 * (i * world_size + j)));
            }
        }
    }


    // Initialize mutex and condition variables
    ASSERT_SYS_OK(pthread_mutex_init(&mutex, NULL));
    ASSERT_SYS_OK(pthread_cond_init(&main_waiting, NULL));

    // Aloc memory for buffers
    send_buffer = malloc(BUFF_SIZE);
    assert(send_buffer != NULL);

    // Aloc memory for receive buffers
    receive_buffer = malloc(sizeof(void*) * world_size);
    assert(receive_buffer != NULL);
    for (int i = 0; i < world_size; i++) {
        receive_buffer[i] = malloc(BUFF_SIZE);
        assert(receive_buffer[i] != NULL);
    }

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
    
    // Create reading threads
    reading_threads = malloc(sizeof(pthread_t) * world_size);
    args = malloc(sizeof(int) * world_size);
    for (int i = 0; i < world_size; i++) {
        if (i != world_rank) {
            args[i] = i;
            ASSERT_SYS_OK(pthread_create(&reading_threads[i], NULL, reading_thread, &args[i]));
        }
    }
    
}


void MIMPI_Finalize() {
    // Close all reading pipes
    for (int i = 0; i < world_size; i++) {
        if (i != world_rank) {
            ASSERT_SYS_OK(close(read_pipe_dsc[i]));
        }
    }

    // Close all the writing pipes
    for (int i = 0; i < world_size; i++) {
        if (i != world_rank) {
            ASSERT_SYS_OK(close(write_pipe_dsc[i]));
        }
    }
    
    // Wait for all reading threads
    for (int i = 0; i < world_size; i++) {
        if (i != world_rank) {
            ASSERT_SYS_OK(pthread_join(reading_threads[i], NULL));
        }
    }

    // Close mutex and condition variables
    ASSERT_SYS_OK(pthread_mutex_destroy(&mutex));
    ASSERT_SYS_OK(pthread_cond_destroy(&main_waiting));

    // Free memory
    free(args);
    free(reading_threads);
    free(read_pipe_dsc);
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
    *(int*)send_buffer = tag;
    *(int*)(send_buffer + sizeof(int)) = count;

    if (count == 0) {
        int ret = chsend(write_pipe_dsc[destination], send_buffer, 2 * sizeof(int));
        if (ret == -1 && (errno == EBADF || errno == EPIPE)) {
            return MIMPI_ERROR_REMOTE_FINISHED;
        }
        ASSERT_SYS_OK(ret);
    }

    while (count > 0) {
        int to_write = count > BUFF_SIZE - 3 * sizeof(int) ? BUFF_SIZE - 3 * sizeof(int) : count;
        *(int*)(send_buffer + 2 * sizeof(int)) = to_write;
        memcpy(send_buffer + 3 * sizeof(int), data_index, to_write);
        int ret = chsend(write_pipe_dsc[destination], send_buffer, to_write + 3 * sizeof(int));
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
    if (source == world_rank && (tag == BARRIER_TAG 
        || tag == BROKEN_BARRIER_TAG || tag == BCAST_TAG)) {
        return MIMPI_SUCCESS;
    }
    if (source == world_rank) {
        return MIMPI_ERROR_ATTEMPTED_SELF_OP;
    }

    // Mutex lock
    ASSERT_SYS_OK(pthread_mutex_lock(&mutex));

    receive_from = source;

    while (!look_for_message(source, tag, count) && finished_processes[source] == 0 
            && broken_barrier == 0) {
        ASSERT_SYS_OK(pthread_cond_wait(&main_waiting, &mutex));
    }

    receive_from = -1;

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
    for (int i = 0; i < log2(world_size); i++) {

        int partner = (world_rank + (1 << i)) % world_size;
    
        if (value_to_return == MIMPI_SUCCESS)
            ret = MIMPI_Send(NULL, 0, partner, BARRIER_TAG);
        else if (value_to_return == MIMPI_ERROR_REMOTE_FINISHED) 
            MIMPI_Send(NULL, 0, partner, BROKEN_BARRIER_TAG);
        if (ret == MIMPI_ERROR_REMOTE_FINISHED) 
            value_to_return = MIMPI_ERROR_REMOTE_FINISHED;

        int previous = (world_rank - (1 << i) + world_size) % world_size;

        if (value_to_return == MIMPI_SUCCESS) 
            ret = MIMPI_Recv(NULL, 0, previous, BARRIER_TAG);
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
    int left = 2 * world_rank + 1;
    int right = 2 * world_rank + 2;
    int parent = (world_rank - 1) / 2;
    if (root < 0 || root >= world_size) {
        return MIMPI_ERROR_NO_SUCH_RANK;
    }
    if (root == world_rank && world_rank != 0) {
        MIMPI_Send(data, count, 0, BCAST_TAG);
    }
    if (world_rank == 0 && root != 0) {
        MIMPI_Recv(data, count, root, BCAST_TAG);
    }
    if (world_rank > 0) {
        MIMPI_Recv(data, count, parent, BCAST_TAG);
    }
    if (left < world_size) {
        MIMPI_Send(data, count, left, BCAST_TAG);
    }
    if (right < world_size) {
        MIMPI_Send(data, count, right, BCAST_TAG);
    }
    if (left < world_size) {
        MIMPI_Recv(NULL, 0, left, BARRIER_TAG);
    }
    if (right < world_size) {
        MIMPI_Recv(NULL, 0, right, BARRIER_TAG);
    }
    if (world_rank != 0) {
        MIMPI_Send(NULL, 0, parent, BARRIER_TAG);
    }
    // TODO BARIERA
    return MIMPI_SUCCESS;
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