#include "config.h"

#include <stdio.h>
#include <stdlib.h>
#include <math.h>

#include "osmtypes.h"
#include "output.h"
#include "node-ram-cache.h"

#include "lmdb.h"

#define CHECK(expr, label) \
    if (MDB_SUCCESS != (ret = (expr))) \
    { \
        fprintf(stderr, "%s failed: (%d) %s, at %s:%d in %s().\n", \
            #expr, ret, mdb_strerror(ret), __FILE__, __LINE__, __func__); \
        goto label; \
    }

#define MAX_DIRTY_WRITES 1000000L

void ensure_transaction();
void commit_transaction(int force);


MDB_env *env;
MDB_dbi dbi;

MDB_txn *txn = NULL;
MDB_cursor *mc = NULL;
long dirty_writes = 0;

osmid_t max_id = 0;


void init_node_persistent_cache(const struct output_options *options, int append)
{
    MDB_txn * txn;
    int ret;

    CHECK(mdb_env_create(&env), err);
    CHECK(mdb_env_set_mapsize(env, 100L * 1024 * 1024 * 1024), err); // 30 GB.
    CHECK(mdb_env_open(env, options->flat_node_file, MDB_NOSYNC, 0664), err);
    CHECK(mdb_txn_begin(env, NULL, 0, &txn), err);
    CHECK(mdb_dbi_open(txn, NULL, MDB_INTEGERKEY, &dbi), err2);
    CHECK(mdb_txn_commit(txn), err2);

    return;

err2:
    mdb_txn_abort(txn);

err:
    mdb_env_close(env);
    exit_nicely();
}

void shutdown_node_persistent_cache()
{
    commit_transaction(1);
    mdb_env_close(env);
}

void ensure_transaction()
{
    int ret;

    if (txn == NULL)
    {
        CHECK(mdb_txn_begin(env, NULL, 0, &txn), err);
        CHECK(mdb_cursor_open(txn, dbi, &mc), err);
        dirty_writes = 0;
    }

    return;

err:
    mdb_txn_abort(txn);
    txn = NULL;
}

void commit_transaction(int force)
{
    int ret;

    if (txn != NULL && (force || (dirty_writes >= MAX_DIRTY_WRITES)))
    {
        mdb_cursor_close(mc);
        CHECK(mdb_txn_commit(txn), err);
        dirty_writes = 0;
        txn = NULL;
    }
    return;

err:
    mdb_txn_abort(txn);
    exit_nicely();
}

int persistent_cache_nodes_get(struct osmNode *out, osmid_t id)
{
    MDB_val key, value;
    int ret;
    struct ramNode * node;

    // fprintf(stderr, "Getting %d\n", id);

    // Force any pending write transaction to commit.
    commit_transaction(1);

    mdb_txn_begin(env, NULL, MDB_RDONLY, &txn);
    key.mv_data = &id;
    key.mv_size = sizeof(osmid_t);
    ret = mdb_get(txn, dbi, &key, &value);
    mdb_txn_abort(txn);
    txn = NULL;

    if (ret == MDB_NOTFOUND)
    {
        return 1;
    }

    if (ret)
    {
        fprintf(stderr, "Unable to read from the node database: %d.\n", ret);
        exit_nicely();
    }

    if (value.mv_size != sizeof(struct ramNode))
    {
        fprintf(stderr, "Unable to read from the node database: returned value is not a ramNode.\n");
        exit_nicely();
    }

    node = (struct ramNode *) value.mv_data;

#ifdef FIXED_POINT
    out->lat = FIX_TO_DOUBLE(node->lat);
    out->lon = FIX_TO_DOUBLE(node->lon);
#else
    out->lat = node->lat;
    out->lon = node->lon;
#endif

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
    MDB_val key, value;
    int flags;
    int ret;

    struct ramNode node;

#ifdef FIXED_POINT
    node.lat = DOUBLE_TO_FIX(lat);
    node.lon = DOUBLE_TO_FIX(lon);
#else
    node.lat = lat;
    node.lon = lon;
#endif

    ensure_transaction();

    key.mv_data = &id;
    key.mv_size = sizeof(osmid_t);
    value.mv_data = &node;
    value.mv_size = sizeof(struct ramNode);

    flags = 0;
    if (id > max_id)
    {
        flags |= MDB_APPEND;
        max_id = id;
    }
    CHECK(mdb_cursor_put(mc, &key, &value, flags), err);
    dirty_writes++;
    commit_transaction(0);
    return 0;

err:
    exit_nicely();
}
