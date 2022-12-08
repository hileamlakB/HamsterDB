#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>
#include <stdbool.h>
#include <data_structures.h>
#include <Utils/utils.h>

int binary_search(int n, key_loc_tuple keys[FANOUT], int key)
{
    int left = -1, right = n, mid;

    while (left + 1 < right)
    {
        mid = (left + right) / 2;
        if (keys[mid].key == key)
        {
            return mid;
        }
        else if (keys[mid].key < key)
        {
            left = mid;
        }
        else
        {
            right = mid;
        }
    }

    return right;
}

// returns the closest location such that in the sorted array
// a[location] is the smallest element >= key
int retrive_location(Btree_node *btree, int key)
{
    // iteratively find through a B-tree
    // the closest location such that in the sorted array
    // a[location] is the smallest element >= key
    int pos;

    if (btree->numKeys == 0)
    {
        return 0;
    }

    pos = binary_search(btree->numKeys, btree->keys, key);

    if (pos < btree->numKeys && btree->keys[pos].key == key)
    {
        return btree->keys[pos].location;
    }
    else
    {
        if (btree->isLeaf)
        {
            return btree->keys[pos].location;
        }
        return retrive_location(btree->children[pos], key);
    }
}

void bt_print(Btree_node *btree)
{
    // print the tree in breadth first order
    // print the keys in each node
    Btree_node *queue[1000];
    int front = 0;
    int back = 0;

    queue[back++] = btree;

    while (front < back)
    {
        int level_size = back - front;
        for (int i = 0; i < level_size; i++)
        {
            Btree_node *node = queue[front++];
            int j;
            for (j = 0; j < node->numKeys; j++)
            {
                printf("(%d, %d) ", node->keys[j].key, node->keys[j].location);
            }
            printf("| ");
            if (!node->isLeaf)
            {
                for (j = 0; j < node->numKeys + 1; j++)
                {
                    queue[back++] = node->children[j];
                }
            }
        }
        printf("\n");
    }
}

Btree_node *bt_create(void)
{
    Btree_node *btree = malloc(sizeof(Btree_node *));

    btree->isLeaf = 1;
    btree->numKeys = 0;

    return btree;
}

void bt_free(Btree_node *btree)
{
    int i;

    if (!btree->isLeaf)
    {
        for (i = 0; i < btree->numKeys + 1; i++)
        {
            bt_free(btree->children[i]);
        }
    }

    free(btree);
}

int bt_search(Btree_node *btree, int key)
{
    int pos;

    if (btree->numKeys == 0)
    {
        return 0;
    }

    pos = binary_search(btree->numKeys, btree->keys, key);

    if (pos < btree->numKeys && btree->keys[pos].key == key)
    {
        return 1;
    }
    else
    {
        return (!btree->isLeaf && bt_search(btree->children[pos], key));
    }
}

// insert a new key into a tree returns new right sibling if the node splits
// TODO: IMPROVE insertion for sorted input by not searching for the position
Btree_node *bt_insert_internal(Btree_node *btree, int key, int location, key_loc_tuple *median) // median is the middle value in case of split
{
    int pos;
    key_loc_tuple mid;
    Btree_node *right_node;

    pos = binary_search(btree->numKeys, btree->keys, key);

    if (pos < btree->numKeys && btree->keys[pos].key == key)
    {
        // if they key is already in the tree, do nothing
        return NULL;
    }

    if (btree->isLeaf)
    {
        // make a space for the new key by moving all keys >= key up one
        // if this ends up filling the node, we'll split it later
        memmove(&btree->keys[pos + 1], &btree->keys[pos], sizeof(*(btree->keys)) * (btree->numKeys - pos));
        printf("pos: %d, value:%d\n", pos, btree->keys[pos].key);
        btree->keys[pos] = (key_loc_tuple){key, location};
        btree->numKeys++;
    }
    else
    {

        /* insert in child */
        right_node = bt_insert_internal(btree->children[pos], key, location, &mid);

        // if the child split, we need to insert the median key
        if (right_node)
        {

            /* every key above pos moves up one space */
            memmove(&btree->keys[pos + 1], &btree->keys[pos], sizeof(*(btree->keys)) * (btree->numKeys - pos));
            /* new child goes in pos + 1*/
            memmove(&btree->children[pos + 2], &btree->children[pos + 1], sizeof(*(btree->keys)) * (btree->numKeys - pos));

            btree->keys[pos] = (key_loc_tuple){key, location};
            btree->children[pos + 1] = right_node;
            btree->numKeys++;
        }
    }

    // if inserting made the node too big, split it
    if (btree->numKeys >= FANOUT)
    {
        int half = btree->numKeys / 2;

        median->key = btree->keys[half].key;
        median->location = btree->keys[half].location;

        // make a new node for keys > median
        // and copy the keys and children over

        right_node = malloc(sizeof(Btree_node));

        right_node->numKeys = btree->numKeys - half - 1;
        right_node->isLeaf = btree->isLeaf;

        memmove(right_node->keys, &btree->keys[half + 1], sizeof(*(btree->keys)) * right_node->numKeys);
        if (!btree->isLeaf)
        {
            // divide children as well if this is not a leaf
            memmove(right_node->children, &btree->children[half + 1], sizeof(*(btree->children)) * (right_node->numKeys + 1));
        }

        btree->numKeys = half;

        return right_node;
    }

    return NULL;
}

void bt_insert(Btree_node *btree, int key, int location)
{
    Btree_node *left_node;  /* new left child */
    Btree_node *right_node; /* new right child */
    key_loc_tuple median;

    right_node = bt_insert_internal(btree, key, location, &median);

    if (right_node)
    {
        // if the root split, make a new root

        left_node = malloc(sizeof(Btree_node));

        /* copy root to left_node */
        memmove(left_node, btree, sizeof(*btree));

        /* make root point to left_node and b2 */
        btree->numKeys = 1;
        btree->isLeaf = 0;
        btree->keys[0].key = median.key;
        btree->keys[0].location = median.location;
        btree->children[0] = left_node;
        btree->children[1] = right_node;
    }
}

serialized_node **bt_serialize(Btree_node *btree, serialized_node **storage)
{
    // serializze node in a bfs fashion for easier retrival
    Btree_node queue[1000]; // here assuming the max number of nodes is 1000 (arbitraryly choosen)
    int front = 0, rear = 0;
    int size = 0;

    queue[rear++] = *btree;
    while (front <= rear)
    {
        int level_length = rear - front;
        for (int i = 0; i < level_length; i++)
        {
            Btree_node *node = &queue[front++];
            storage[size]->num_keys = node->numKeys;
            storage[size]->is_leaf = node->isLeaf;
            memcpy(storage[size]->keys, node->keys, sizeof(node->keys));
            size++;

            if (!node->isLeaf)
            {
                for (int j = 0; j <= node->numKeys; j++)
                {
                    queue[rear++] = *node->children[j];
                }
            }
        }
    }

    return storage;
}

Btree_node *bt_deserialize(serialized_node **s_node)
{
    int front = 0, back = 0;
    int seralized_index = 0;
    Btree_node *btree = malloc(sizeof(Btree_node));

    Btree_node *queue[1000];

    queue[back++] = btree;

    while (front < back)
    {

        int level_length = back - front;

        for (int i = 0; i < level_length; i++)
        {

            serialized_node *s_node_ = s_node[seralized_index];
            Btree_node *node = queue[front++];
            node->numKeys = node->numKeys;
            node->isLeaf = node->isLeaf;
            memcpy(node->keys, s_node_->keys, sizeof(s_node_->keys));
            seralized_index++;

            if (!node->isLeaf)
            {
                for (int j = 0; j <= node->numKeys; j++)
                {
                    node->children[j] = malloc(sizeof(Btree_node));
                    queue[back++] = node->children[j];
                }
            }
        }
    }
    return btree;
}
