#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "osmtypes.h"
#include "output.h"
#include "node-ram-cache.h"

#include "leveldb/c.h"

leveldb_t *db;

void init_node_persistent_cache(const struct output_options *options, int append)
{
    leveldb_options_t *db_options;
    char *err = NULL;

    db_options = leveldb_options_create();
    leveldb_options_set_write_buffer_size(db_options, 128 * 1024 * 1024);
    leveldb_options_set_cache(db_options, 16 * 1024 * 1024);
    leveldb_options_set_create_if_missing(db_options, 1);
    db = leveldb_open(db_options, options->flat_node_file, &err);

    if (err != NULL)
    {
        fprintf(stderr, "Failed to open node cache database: %s\n", err);
        leveldb_options_destroy(db_options);
        free(err);
        exit_nicely();
    }
}

void shutdown_node_persistent_cache()
{
    leveldb_close(db);
}

int persistent_cache_nodes_get(struct osmNode *out, osmid_t id)
{
    leveldb_readoptions_t *roptions;
    char *read;
    struct ramNode * node;
    char *err = NULL;
    size_t read_len;

    roptions = leveldb_readoptions_create();
    read = leveldb_get(db, roptions, (char *) &id, sizeof(osmid_t), &read_len, &err);

    // fprintf(stderr, "Getting %d\n", id);

    if (err != NULL)
    {
        fprintf(stderr, "Unable to read from the node database: %s.\n", err);
        leveldb_readoptions_destroy(roptions);
        free(err);
        exit_nicely();
    }

    if (read == NULL)
    {
        // Not found.
        return 1;
    }

    if (read_len != sizeof(struct ramNode))
    {
        fprintf(stderr, "Unable to read from the node database: returned value is not a ramNode.\n");
        leveldb_readoptions_destroy(roptions);
        free(read);
        exit_nicely();
    }

    node = (struct ramNode *) read;

#ifdef FIXED_POINT
    out->lat = FIX_TO_DOUBLE(node->lat);
    out->lon = FIX_TO_DOUBLE(node->lon);
#else
    out->lat = node->lat;
    out->lon = node->lon;
#endif

    free(read);
    leveldb_readoptions_destroy(roptions);

    return 0;
}

int persistent_cache_nodes_get_list(struct osmNode *nodes, osmid_t *ndids,
        int nd_count)
{
    int count = 0;
    int i;

    // fprintf(stderr, "Getting list of %d nodes\n", nd_count);

    for (i = 0; i < nd_count; i++)
    {
        if (!persistent_cache_nodes_get(&nodes[count], ndids[i])) {
            count++;
        }
    }

    // fprintf(stderr, "Done getting list, fetched: %d\n", count);

    return count;
}

int persistent_cache_nodes_set(osmid_t id, double lat, double lon)
{
    leveldb_writeoptions_t *woptions;
    struct ramNode * node;
    char *err = NULL;

    node = malloc(sizeof(struct ramNode));
    if (!node) {
        fprintf(stderr, "Out of memory: Failed to allocate node writeout buffer\n");
        exit_nicely();
    }

#ifdef FIXED_POINT
    node->lat = DOUBLE_TO_FIX(lat);
    node->lon = DOUBLE_TO_FIX(lon);
#else
    node->lat = lat;
    node->lon = lon;
#endif

    woptions = leveldb_writeoptions_create();
    leveldb_put(db, woptions, (char*) &id, sizeof(osmid_t), (char*) node, sizeof(node), &err);

    free(node);
    leveldb_writeoptions_destroy(woptions);

    if (err != NULL)
    {
        fprintf(stderr, "Failed to write to the node cache database: %s\n", err);
        free(err);
        exit_nicely();
    }
}
