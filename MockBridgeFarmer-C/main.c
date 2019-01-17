#include "genarotests.h"

int create_test_upload_file(char *filepath)
{
    FILE *fp;
    fp = fopen(filepath, "wb+");

    if (fp == NULL) {
        printf(KRED "Could not create upload file: %s\n" RESET, filepath);
        exit(0);
    }

    int shard_size = 16777216;
    char *bytes = "abcdefghijklmn";
    for (int i = 0; i < strlen(bytes); i++) {
        char *page = calloc(shard_size + 1, sizeof(char));
        memset(page, bytes[i], shard_size);
        fputs(page, fp);
        free(page);
    }

    fclose(fp);
    return 0;
}

// Test Bridge Server
struct MHD_Daemon *start_test_server()
{
    // spin up test bridge server
    return MHD_start_daemon(MHD_USE_THREAD_PER_CONNECTION,
                            8091,
                            NULL,
                            NULL,
                            &mock_bridge_server,
                            NULL,
                            MHD_OPTION_END);
}

int main(void)
{
    // Make sure we have a tmp folder
    char *folder = getenv("TMPDIR");
	
	if (folder == 0) {
        struct stat sb;
		if (stat("/tmp/", &sb) == 0 && S_ISDIR(sb.st_mode)) {
	        folder = "/tmp/";
	    } else {
	        printf("You need to set $TMPDIR before running. (e.g. export TMPDIR=/tmp/)\n");
	        exit(1);
	    }
	}

    char *file_name = "genaro-test-upload.data";
    int len = strlen(folder) + 1 + strlen(file_name);
    char *file = calloc(len + 1, sizeof(char));
    strcpy(file, folder);
    strcat(file, "/");
    strcat(file, file_name);
    file[len] = '\0';

    create_test_upload_file(file);

    // spin up test bridge server
    struct MHD_Daemon *d = start_test_server();
    if (d == NULL) {
        printf("Could not start test server.\n");
        return 0;
    };

    // spin up test farmer server
    struct MHD_Daemon *f = start_farmer_server();

    printf("bridge and farmer are running...\n");

//    sleep(1000);
//
//    // Shutdown test servers
//    MHD_stop_daemon(d);
//    MHD_stop_daemon(f);
//    free_farmer_data();

//    printf("bridge and farmer are stopped\n");
    sleep(-1);

    return 0;
}
