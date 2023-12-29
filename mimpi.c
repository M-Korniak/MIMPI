/**
 * This file is for implementation of MIMPI library.
 * */
#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <fcntl.h>

#include "channel.h"
#include "mimpi.h"
#include "mimpi_common.h"


static int world_size;
static int world_rank;
static int read_pipe_dsc;
static int* write_pipe_dsc;

void MIMPI_Init(bool enable_deadlock_detection) {
    channels_init();

    world_size = atoi(getenv("MIMPI_WORLD_SIZE"));
    world_rank = atoi(getenv("MIMPI_WORLD_RANK"));

    read_pipe_dsc = 20 + 2 * world_rank;
    write_pipe_dsc = malloc(sizeof(int) * world_size);

    for (int i = 0; i < world_size; i++) {
        write_pipe_dsc[i] = 21 + 2 * i;
    }

    //close reading pipes
    for (int i = 20; i < 20 + 2 * world_size; i += 2) {
        if (i != 20 + 2 * world_rank) {
            ASSERT_SYS_OK(close(i));
        }
    }
    //close this process writing pipe
    ASSERT_SYS_OK(close(write_pipe_dsc[world_rank]));

    for (int i = 20; i < 1024; i++) {
        if (fcntl(i, F_GETFD) != -1) {
            printf("File descriptor %d is open in process %d.\n", i, world_rank);
        }
    }

    
}

void MIMPI_Finalize() {
    
    ASSERT_SYS_OK(close(read_pipe_dsc));
    for (int i = 0; i < world_size; i++) {
        if (i != world_rank) {
            ASSERT_SYS_OK(close(write_pipe_dsc[i]));
        }
    }
    free(write_pipe_dsc);
    
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
    TODO
}

MIMPI_Retcode MIMPI_Recv(
    void *data,
    int count,
    int source,
    int tag
) {
    TODO
}

MIMPI_Retcode MIMPI_Barrier() {
    TODO
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