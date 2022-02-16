import random

class InternalNode(object):

    def __init__(self, parent=None):
        # Inner Nodes that hold keys+vals
        self.keys = []
        self.values: list[InternalNode] = []  # Can only be list of INTERNAL NODES
        self.parent: InternalNode = parent

    def index(self, column_number):
        # given key/column number, will return index where its at
        for index, key in enumerate(self.keys):
            if column_number < key:
                return index

        return len(self.keys)

    def split(self):
        """
        Splits the node into two and stores them as child nodes.
        extract a pivot from the child to be inserted into the keys of the parent.
        """

        # set internal nodes for splitting
        leftSibling = InternalNode()
        rightSibling = InternalNode()
        threshold = int(len(self.keys) // 2)

        # make both new internal nodes go back to same parent
        leftSibling.parent = rightSibling.parent = self

        # give left sibling first half of keys/values
        leftSibling.keys = self.keys[:threshold]
        leftSibling.values = self.values[:threshold + 1]

        # go through left sibling values and set to corect parent if needed
        for value in leftSibling.values:
            # if isinstance(value, InternalNode):
            if value:
                value.parent = leftSibling

        # give right sibling second half of keys/values
        rightSibling.keys = self.keys[threshold + 1:]
        rightSibling.values = self.values[threshold + 1:]

        # go through and set correct parent for rightSibling values
        for value in rightSibling.values:
            # if isinstance(value, InternalNode):
            if value:
                value.parent = rightSibling

        # reassign self.values to new internal nodes and set middle value as precursor to LeafNode
        key = self.keys[threshold]
        self.keys = rightSibling.keys
        self.values = rightSibling.values

        return key, [leftSibling, self]

    def mergeLeft(self, index):
        nextSibling: InternalNode = self.parent.values[index + 1]

        nextSibling.keys[0:0] = self.keys + [self.parent.keys[index]]  # is 0:0 needed?
        for value in self.values:
            value.parent = nextSibling
        nextSibling.values[0:0] = self.values
        pass


    def mergeRight(self):
        previousSibling: InternalNode = self.parent.values[-2]
        previousSibling.keys = previousSibling.keys + self.keys + [self.parent.keys[-1]]
        for value in self.values:
            value.parent = previousSibling
        previousSibling.values += self.values
        pass


    def merge(self):
        # Merges Nodes together
        index = self.parent.index(self.keys[0])
        # merge this node with the next node
        if index < len(self.parent.keys):
            self.mergeLeft(index)
        else:  # If self is the last node, merge with prev
            self.mergeRight()
            pass


    def borrowLeft(self, index, minVal):
        nextSibling: InternalNode = self.parent.values[index + 1]


        if len(nextSibling.keys) > minVal:
            self.keys += [self.parent.keys[index]]

            borrowSibling = nextSibling.values.pop(0)
            borrowSibling.parent = self
            self.values += [borrowSibling]
            self.parent.keys[index] = nextSibling.keys.pop(0)
            return True


    def borrowRight(self, index, minVal):
        previousSibling: InternalNode = self.parent.values[index - 1]

        if len(previousSibling.keys) > minVal:
            self.keys[0:0] = [self.parent.keys[index - 1]]

        borrowSibling = previousSibling.values.pop()
        borrowSibling.parent = self
        self.values[0:0] = [borrowSibling]
        self.parent.keys[index - 1] = previousSibling.keys.pop()
        return True


    def borrowKey(self, minVal):
        index = self.parent.index(self.keys[0])
        if index < len(self.parent.keys):
            self.borrowLeft(index, minVal)
        elif index != 0:
            self.borrowRight(index, minVal)
        return False


    def __getitme__(self, value):
        # fetch value
        return self.values[self.index(value)]


    def __setitem__(self, key, value):
        # set value to something else
        index = self.index(key)
        self.keys[index:index] = [key]
        self.values.pop(index)
        self.values[index:index] = value


    def __delete__(self, key):
        # removes value from self
        index = self.index(key)
        self.values.pop(index)
        if index < len(self.keys):
            self.keys.pop(index)
        else:
            self.keys.pop(index - 1)


class LeafNode(InternalNode):
    def __init__(self, parent=None, rightSibling=None, leftSibling=None):
        """
        Create a new leaf in the leaf link
        """
        super(LeafNode, self).__init__(parent)
        self.nextLeaf: LeafNode = leftSibling
        if leftSibling is not None:
            leftSibling.prev = self
        self.previousLeaf: LeafNode = rightSibling
        if rightSibling is not None:
            rightSibling.next = self

    def split_leaves(self):
        left = LeafNode(self.parent, self.previousLeaf, self)
        right = LeafNode(self.parent, self.nextLeaf, self)
        threshold = int(len(self.keys) // 2)

        left.keys = self.keys[:threshold]
        left.values = self.values[:threshold]

        right.keys = self.keys[threshold:]
        right.values = self.values[threshold:]

        self.keys = right.keys
        self.values = right.values
        # When the leaf node is split, set the parent key to the left-most key of the right child node.
        return self.keys[0], [left, self]

    def __getitem__(self, item):
        return self.values[self.keys.index(item)]

    def __setitem__(self, key, value):
        index = self.index(key)
        if key not in self.keys:
            self.keys[index:index] = [key]
            self.values[index:index] = [value]
        else:
            self.values[index - 1] = value

    def __delitem__(self, key):
        index = self.keys.index(key)
        del self.keys[index]
        del self.values[index]

    def merge(self):

        if self.nextLeaf is not None and self.nextLeaf.parent == self.parent:
            self.nextLeaf.keys[0:0] = self.keys
            self.nextLeaf.values[0:0] = self.values
        else:
            self.previousLeaf.keys += self.keys
            self.previousLeaf.values += self.values

        if self.nextLeaf is not None:
            self.nextLeaf.previousLeaf = self.previousLeaf
        if self.previousLeaf is not None:
            self.previousLeaf.nextLeaf = self.nextLeaf
        pass

    def borrowLeftLeaf(self,index):
        self.keys += [self.nextLeaf.keys.pop(0)]
        self.values += [self.nextLeaf.values.pop(0)]
        self.parent.keys[index] = self.nextLeaf.keys[0]
        return True

    def borrowRightLeaf(self,index):
        self.keys[0:0] = [self.previousLeaf.keys.pop()]
        self.values[0:0] = [self.previousLeaf.values.pop()]
        self.parent.keys[index - 1] = self.keys[0]
        return True

    def borrowKey(self, minVal):
        index = self.parent.index(self.keys[0])
        if index < len(self.parent.keys) and len(self.nextLeaf.keys) > minVal:
            self.borrowLeftLeaf(index)
        elif index != 0 and len(self.previousLeaf.keys) > minVal:
            self.borrowRightLeaf(index)
        return False


class BTree(object):
    """
    B+ tree object, consisting of nodes.
    Nodes will automatically be split into two once it is full. When a split occurs, a key will
    'float' upwards and be inserted into the parent node to act as a pivot.
    Attributes:
    """
    root: InternalNode

    def __init__(self, max_limit=512):
        self.root = LeafNode()
        self.max_limit = max_limit if max_limit > 2 else 2
        self.min_limit = self.max_limit // 2
        self.depth = 0

    def find(self, key) -> LeafNode:
        """ find the leaf
        Returns:
            Leaf: the leaf which should have the key
        """
        root_node = self.root
        # Traverse tree until leaf node is reached.
        while type(root_node) is not LeafNode:
            root_node = root_node[key]
        return root_node

    def __getitem__(self, value):
        return self.find(value)[value]

    def query(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        leaf_node = self.find(key)
        return leaf_node[key] if key in leaf_node.keys else None

    def change(self, key, value):
        """change the value
        Returns:
            (bool,Leaf): the leaf where the key is. return False if the key does not exist
        """
        leaf_node = self.find(key)
        if key not in leaf_node.keys:
            return False, leaf_node
        else:
            leaf_node[key] = value
            return True, leaf_node

    def __setitem__(self, key, value, leaf_node=None):
        """
        Inserts a key-value pair after traversing to a leaf node. If the leaf node is full, split
              the leaf node into two.
        """
        if leaf_node is None:
            leaf_node = self.find(key)
        leaf_node[key] = value
        if len(leaf_node.keys) > self.max_limit:
            self.insert_index(*leaf_node.split())

    def insert(self, key, value):
        """
        Returns:
            (bool,Leaf): the leaf where the key is inserted. return False if already has same key
        """
        leaf_node = self.find(key)
        if key in leaf_node.keys:
            return False, leaf_node
        else:
            self.__setitem__(key, value, leaf_node)
            return True, leaf_node

    def insert_index(self, key, values: list[InternalNode]):
        """For a parent and child node,
                    Insert the values from the child into the values of the parent."""
        parent = values[1].parent
        if parent is None:
            # values[0].parent = values[1].parent = self.root = InternalNode()
            values[0].parent = InternalNode()
            values[1].parent = InternalNode()
            self.root = InternalNode()
            self.depth += 1
            self.root.keys = [key]
            self.root.values = values
            pass

        parent[key] = values
        # If the node is full, split the  node into two.
        if len(parent.keys) > self.max_limit:
            self.insert_index(*parent.split())
        # Once a leaf node is split, it consists of a internal node and two leaf nodes.
        # These need to be re-inserted back into the tree.

    def delete(self, key, root_node: InternalNode = None):
        if root_node is None:
            root_node = self.find(key)
        del root_node[key]

        if len(root_node.keys) < self.min_limit:
            if root_node == self.root:
                if len(self.root.keys) == 0 and len(self.root.values) > 0:
                    self.root = self.root.values[0]
                    self.root.parent = None
                    self.depth -= 1
                return

            elif not root_node.borrowKey(self.min_limit):
                root_node.merge()
                self.delete(key, root_node.parent)
        # Change the left-most key in node
        # if i == 0:
        #     node = self
        #     while i == 0:
        #         if node.parent is None:
        #             if len(node.keys) > 0 and node.keys[0] == key:
        #                 node.keys[0] = self.keys[0]
        #             return
        #         node = node.parent
        #         i = node.index(key)
        #
        #     node.keys[i - 1] = self.keys[0]

    def show(self, node=None, file=None, _prefix="", _last=True):
        """Prints the keys at each level."""
        if node is None:
            node = self.root
        print(_prefix, "`- " if _last else "|- ", node.keys, sep="", file=file)
        _prefix += "   " if _last else "|  "

        if type(node) is InternalNode:
            # Recursively print the key of child nodes (if these exist).
            for i, child in enumerate(node.values):
                _last = (i == len(node.values) - 1)
                self.show(child, file, _prefix, _last)

    def readfile(self, reader):
        i = 0
        for i, line in enumerate(reader):
            s = line.decode().split(maxsplit=1)
            self[s[0]] = s[1]
            if i % 1000 == 0:
                print('Insert ' + str(i) + 'items')
        return i + 1

    def leftmost_leaf(self) -> LeafNode:
        root_node = self.root
        while type(root_node) is not LeafNode:
            root_node = root_node.values[0]
        return root_node


def demo():
    bplustree = BTree()
    random_list = random.sample(range(1, 100), 20)
    for i in random_list:
        bplustree[i] = 'test' + str(i)
        print('Insert ' + str(i))
        bplustree.show()
    random.shuffle(random_list)
    for i in random_list:
        print('Delete ' + str(i))
        bplustree.delete(i)
        bplustree.show()


if __name__ == '__main__':
    demo()