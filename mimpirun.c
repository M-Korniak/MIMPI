/**
 * This file is for implementation of mimpirun program.
 * */

#include <stdlib.h>
#include <unistd.h>
#include <stdio.h>
#include <sys/wait.h>

#include "mimpi_common.h"
#include "channel.h"

int main(int argc, char* argv[]) {
    if (argc < 3)
       fatal("Usage: %s <number of processes> <program to run> [program arguments...]\n", argv[0]);
    int const n = atoi(argv[1]);
    const char* program_to_run = argv[2];
    char** program_args = &argv[2];

    ASSERT_SYS_OK(setenv("MIMPI_WORLD_SIZE", argv[1], 1));

    int pipes[n * n][2];

    for (int i = 0; i < n * n; i++) {
        ASSERT_SYS_OK(channel(pipes[i]));
        if (pipes[i][1] != 20 + 2 * i) {
            ASSERT_SYS_OK(dup2(pipes[i][0], 20 + 2 * i));
            if (pipes[i][0] != 20 + 2 * i)
                ASSERT_SYS_OK(close(pipes[i][0]));
            ASSERT_SYS_OK(dup2(pipes[i][1], 21 + 2 * i));
            if (pipes[i][1] != 21 + 2 * i)
                ASSERT_SYS_OK(close(pipes[i][1]));
        }
        else {
            ASSERT_SYS_OK(dup2(pipes[i][1], 21 + 2 * i));
            if (pipes[i][1] != 21 + 2 * i)
                ASSERT_SYS_OK(close(pipes[i][1]));
            ASSERT_SYS_OK(dup2(pipes[i][0], 20 + 2 * i));
            if (pipes[i][0] != 20 + 2 * i)
                ASSERT_SYS_OK(close(pipes[i][0]));
        }
    }

    // Create n children
    for (int i = 0; i < n; i++) {
        pid_t pid = fork();
        ASSERT_SYS_OK(pid);
        if (pid == 0) {
            char buf[4];
            int ret = snprintf(buf, sizeof(buf), "%d", i);
            if (ret < 0 || ret >= (int)sizeof(buf))
                fatal("Error in snprintf.");
            ASSERT_SYS_OK(setenv("MIMPI_WORLD_RANK", buf, 1));
            ASSERT_SYS_OK(execvp(program_to_run, program_args));
        }
    }
    // Close all pipes
    for (int i = 20; i < 20 + 2 * n * n; i++) {
        ASSERT_SYS_OK(close(i));
    }
    // Wait for all children
    for (int i = 0; i < n; i++) {
        ASSERT_SYS_OK(wait(NULL));
    }

    return 0;
}