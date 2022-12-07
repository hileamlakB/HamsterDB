import bisect

max_size = 5
leaf_size = 5


class Node:
    def __init__(self, is_leaf=True, is_root=False, keys=[], children=[]):
        self.keys = keys
        self.children = children
        self.is_leaf = is_leaf
        self.is_root = is_root

    def __repr__(self):
        return "Node(leaf:{},keys:{})".format(self.is_leaf, self.keys)


class Btree:
    def __init__(self, root=None):
        self.root = root


def insert_at(array, index, value):
    array.append(None)
    for i in range(len(array) - 1, index - 1, -1):
        array[i] = array[i - 1]
    array[index] = value


def insert_location(array, value):
    left, middle = 0, 0
    right = len(array) - 1
    while (left <= right):
        middle = (left + right) // 2

        if (array[middle] < value):
            left = middle + 1
        else:
            right = middle - 1
    return left


def __insert(tree, key):
    root = tree.root

    if root.is_leaf:
        if len(root.keys) >= leaf_size:
            return split_up(root)

        bisect.insort(root.keys, key)
        return

    location = insert_location(root.keys, key)

    # this means key is greater than the last key in the node
    tmp_tree = Btree(root=root.children[location])
    inserted = __insert(tmp_tree, key)
    if (inserted == None):
        return

    l, k, r = inserted
    insert_at(root.children, location + 1, r)
    insert_at(root.keys, location, k)

    if (len(root.keys) == max_size):
        return split_up(root)

    return __insert(tree, key)


def insert(tree, key):
    root = tree.root
    if (not root):
        root = Node(False, True,
                    keys=[key],
                    children=[Node(keys=[key]), Node()])
        tree.root = root
        return

    not_inserted = __insert(tree, key)
    if (not_inserted):
        l, k, r = not_inserted

        new_root = Node(is_root=True, is_leaf=False, keys=[k], children=[l, r])
        tree.root = new_root
        __insert(tree, key)
    return


def split_up(root):

    if (root.is_leaf):
        size = leaf_size
    else:
        size = max_size

    new_btree = Node(is_leaf=root.is_leaf,
                     keys=root.keys[size // 2:], children=root.children[size // 2:])

    root.keys = root.keys[:size // 2]
    root.children = root.children[:size // 2]

    return (root, root.keys[-1], new_btree)


def print_btree(node):

    string = ""
    queue = [node]
    while queue:
        level = len(queue)
        for i in range(level):
            node = queue.pop(0)
            string += str(node.keys) + " "
            if not node.is_leaf:
                for i in range(len(node.children)):
                    queue.append(node.children[i])

        string += "\n"
    print(string)


my_btree = Btree()

insert(my_btree, 1)
insert(my_btree, 2)
insert(my_btree, 3)
insert(my_btree, 4)
insert(my_btree, 5)
insert(my_btree, 0)
insert(my_btree, 6)
insert(my_btree, 7)
insert(my_btree, 8)
insert(my_btree, 9)
# insert(my_btree, 2.4)
# insert(my_btree, 2.3)
# insert(my_btree, 2.2)
# insert(my_btree, 2.5)
# insert(my_btree, 10)
# insert(my_btree, 11)
# insert(my_btree, 8.5)
# insert(my_btree, 8.4)
# insert(my_btree, 2.35)
# insert(my_btree, 2.357)
# insert(my_btree, 2.4)
# insert(my_btree, 2.6)
# insert(my_btree, 0.4)
# insert(my_btree, 0.8)
# insert(my_btree, 0.9)
# insert(my_btree, 0.10)
# insert(my_btree, 50)
# insert(my_btree, 510)
# insert(my_btree, 520)
# insert(my_btree, 520)
# insert(my_btree, 519)
# insert(my_btree, 518)
# insert(my_btree, 4)
# insert(my_btree, 2.9)
# insert(my_btree, 2.7)
# insert(my_btree, 2.7)
# insert(my_btree, 2.7)
# insert(my_btree, 2.7)
# insert(my_btree, 2.7)
# insert(my_btree, 2.7)
# insert(my_btree, 2.7)
# insert(my_btree, 2.7)
# insert(my_btree, 2.8)


print_btree(my_btree.root)
# print(my_btree.root, my_btree.root.children)
