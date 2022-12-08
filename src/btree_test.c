#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include "data_structures.h"

int main()
{
    Btree_node *btree;
    int i;

    btree = bt_create();

    // bt_insert(btree, 12);
    // bt_insert(btree, 13);
    // bt_insert(btree, 15);
    // bt_insert(btree, 24);
    // bt_insert(btree, 11);
    // // bt_print(btree);

    // bt_free(btree);

    btree = bt_create();
    for (i = 0; i < 100; i++)
    {
        if (i == 35)
        {
            bt_insert(btree, i, 8);
        }
        else
        {
            bt_insert(btree, i * 2, i);
        }
    }

    printf("%d\n", retrive_location(btree, 40));
    printf("%d\n", retrive_location(btree, 35));
    printf("%d\n", retrive_location(btree, 70));

    bt_print(btree);
    bt_free(btree);

    return 0;
}