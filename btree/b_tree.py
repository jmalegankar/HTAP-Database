#!/usr/bin/env python3

from collections import deque
import random

class Node(object):
    """Base node object. It should be index node
    Each node stores keys and children.

    Attributes:
        parent
    """
    
    def __init__(self, parent=None):
        """Child nodes are stored in values. Parent nodes simply act as a medium to traverse the tree.
        :type parent: Node"""
        self.keys: list = []
        self.values: list[Node] = []
        self.parent: Node = parent
        
    def index(self, key):
        """Return the index where the key should be.
        :type key: str
        """
        for i, item in enumerate(self.keys):
            if key < item:
                return i
            
        return len(self.keys)
    
    def __getitem__(self, item):
        return self.values[self.index(item)]
    
    def __setitem__(self, key, value):
        i = self.index(key)
        self.keys[i:i] = [key]
        self.values.pop(i)
        self.values[i:i] = value
        
    def split(self):
        """Splits the node into two and stores them as child nodes.
        extract a pivot from the child to be inserted into the keys of the parent.
        @:return key and two children
        """
        
        left = Node(self.parent)
        
        mid = len(self.keys) // 2
        
        left.keys = self.keys[:mid]
        left.values = self.values[:mid + 1]
        for child in left.values:
            child.parent = left
            
        key = self.keys[mid]
        self.keys = self.keys[mid + 1:]
        self.values = self.values[mid + 1:]
        
        return key, [left, self]
    
    def __delitem__(self, key):
        i = self.index(key)
        del self.values[i]
        if i < len(self.keys):
            del self.keys[i]
        else:
            del self.keys[i - 1]
            
    def fusion(self):
        index = self.parent.index(self.keys[0])
        # merge this node with the next node
        if index < len(self.parent.keys):
            next_node: Node = self.parent.values[index + 1]
            next_node.keys[0:0] = self.keys + [self.parent.keys[index]]
            for child in self.values:
                child.parent = next_node
            next_node.values[0:0] = self.values
        else:  # If self is the last node, merge with prev
            prev: Node = self.parent.values[-2]
            prev.keys += [self.parent.keys[-1]] + self.keys
            for child in self.values:
                child.parent = prev
            prev.values += self.values
        
    def borrow_key(self, minimum: int):
        index = self.parent.index(self.keys[0])
        if index < len(self.parent.keys):
            next_node: Node = self.parent.values[index + 1]
            if len(next_node.keys) > minimum:
                self.keys += [self.parent.keys[index]]
                
                borrow_node = next_node.values.pop(0)
                borrow_node.parent = self
                self.values += [borrow_node]
                self.parent.keys[index] = next_node.keys.pop(0)
                return True
        elif index != 0:
            prev: Node = self.parent.values[index - 1]
            if len(prev.keys) > minimum:
                self.keys[0:0] = [self.parent.keys[index - 1]]
                
                borrow_node = prev.values.pop()
                borrow_node.parent = self
                self.values[0:0] = [borrow_node]
                self.parent.keys[index - 1] = prev.keys.pop()
                return True
            
        return False
    
    
class Leaf(Node):
    def __init__(self, parent=None, prev_node=None, next_node=None):
        """
        Create a new leaf in the leaf link
        :type prev_node: Leaf
        :type next_node: Leaf
        """
        super(Leaf, self).__init__(parent)
        self.next: Leaf = next_node
        if next_node is not None:
            next_node.prev = self
        self.prev: Leaf = prev_node
        if prev_node is not None:
            prev_node.next = self
            
    def __getitem__(self, item):
        return self.values[self.keys.index(item)]
    
    def __setitem__(self, key, value):
        i = self.index(key)
        if key not in self.keys:
            self.keys[i:i] = [key]
            self.values[i:i] = [value]
        else:
            self.values[i - 1] = value
            
    def split(self):
        left = Leaf(self.parent, self.prev, self)
        mid = len(self.keys) // 2
        
        left.keys = self.keys[:mid]
        left.values = self.values[:mid]
        
        self.keys: list = self.keys[mid:]
        self.values: list = self.values[mid:]
        
        # When the leaf node is split, set the parent key to the left-most key of the right child node.
        return self.keys[0], [left, self]
    
    def __delitem__(self, key):
        i = self.keys.index(key)
        del self.keys[i]
        del self.values[i]
        
    def fusion(self):
        if self.next is not None and self.next.parent == self.parent:
            self.next.keys[0:0] = self.keys
            self.next.values[0:0] = self.values
        else:
            self.prev.keys += self.keys
            self.prev.values += self.values
            
        if self.next is not None:
            self.next.prev = self.prev
        if self.prev is not None:
            self.prev.next = self.next
            
    def borrow_key(self, minimum: int):
        index = self.parent.index(self.keys[0])
        if index < len(self.parent.keys) and len(self.next.keys) > minimum:
            self.keys += [self.next.keys.pop(0)]
            self.values += [self.next.values.pop(0)]
            self.parent.keys[index] = self.next.keys[0]
            return True
        elif index != 0 and len(self.prev.keys) > minimum:
            self.keys[0:0] = [self.prev.keys.pop()]
            self.values[0:0] = [self.prev.values.pop()]
            self.parent.keys[index - 1] = self.keys[0]
            return True
        
        return False
    
    
class BPlusTree(object):
    """B+ tree object, consisting of nodes.

    Nodes will automatically be split into two once it is full. When a split occurs, a key will
    'float' upwards and be inserted into the parent node to act as a pivot.

    Attributes:
        maximum (int): The maximum number of keys each node can hold.
    """
    root: Node
    
    def __init__(self, maximum=8):
        self.root = Leaf()
        self.maximum: int = maximum if maximum > 2 else 2
        self.minimum: int = self.maximum // 2
        self.depth = 0
        
    def find(self, key) -> Leaf:
        """ find the leaf

        Returns:
            Leaf: the leaf which should have the key
        """
        node = self.root
        # Traverse tree until leaf node is reached.
        while type(node) is not Leaf:
            node = node[key]
            
        return node
    
    def __getitem__(self, item):
        return self.find(item)[item]
    
    def query(self, key):
        """Returns a value for a given key, and None if the key does not exist."""
        leaf = self.find(key)
        return leaf[key] if key in leaf.keys else None
    
    def range_query(self, keyMin, keyMax):
        all_keys = []
        all_values = []
        start_leaf = self.find(keyMin)
        keys, values, next_node = self.get_data_in_key_range(keyMin, keyMax, start_leaf)
        all_keys += keys
        all_values += values
        while next_node:
            keys, values, next_node = self.get_data_in_key_range(keyMin, keyMax, next_node)
            all_keys += keys
            all_values += values
        return all_keys, all_values
    
    def get_data_in_key_range(self, keyMin, keyMax, node):
        keys = []
        values = []
        hasKey = False
        for i, key in enumerate(node.keys):
            hasKey = True
            if keyMin <= key <= keyMax:
                keys.append(key)
                values.append(node.values[i])

        if hasKey:
            if node.keys[-1] > keyMax:
                next_node = None
            else:
                next_node = node.next
            return keys, values, next_node
        else:
            return keys, values, None
    
    def change(self, key, value):
        """change the value

        Returns:
            (bool,Leaf): the leaf where the key is. return False if the key does not exist
        """
        leaf = self.find(key)
        if key not in leaf.keys:
            return False, leaf
        else:
            leaf[key] = value
            return True, leaf
        
    def __setitem__(self, key, value, leaf=None):
        """Inserts a key-value pair after traversing to a leaf node. If the leaf node is full, split
              the leaf node into two.
              """
        if leaf is None:
            leaf = self.find(key)
        leaf[key] = value
        if len(leaf.keys) > self.maximum:
            self.insert_index(*leaf.split())
            
    def insert(self, key, value):
        """
        Returns:
            (bool,Leaf): the leaf where the key is inserted. return False if already has same key
        """
        leaf = self.find(key)
        if key in leaf.keys:
            return False, leaf
        else:
            self.__setitem__(key, value, leaf)
            return True, leaf
        
    def insert_index(self, key, values):
        """For a parent and child node,
                    Insert the values from the child into the values of the parent."""
        parent = values[1].parent
        if parent is None:
            values[0].parent = values[1].parent = self.root = Node()
            self.depth += 1
            self.root.keys = [key]
            self.root.values = values
            return
        
        parent[key] = values
        # If the node is full, split the  node into two.
        if len(parent.keys) > self.maximum:
            self.insert_index(*parent.split())
        # Once a leaf node is split, it consists of a internal node and two leaf nodes.
        # These need to be re-inserted back into the tree.
            
    def delete(self, key, node: Node = None):
        if node is None:
            node = self.find(key)
        del node[key]
        
        if len(node.keys) < self.minimum:
            if node == self.root:
                if len(self.root.keys) == 0 and len(self.root.values) > 0:
                    self.root = self.root.values[0]
                    self.root.parent = None
                    self.depth -= 1
                return
            
            elif not node.borrow_key(self.minimum):
                node.fusion()
                self.delete(key, node.parent)
                
    def show(self, node=None, _prefix="", _last=True):
        """Prints the keys at each level."""
        if node is None:
            node = self.root
        print(_prefix, "`- " if _last else "|- ", node.keys, sep="")
        _prefix += "   " if _last else "|  "
        
        if type(node) is Node:
            # Recursively print the key of child nodes (if these exist).
            for i, child in enumerate(node.values):
                _last = (i == len(node.values) - 1)
                self.show(child, _prefix, _last)

    def list_all(self):
        node = self.root
        stack = deque()
        
        results = []

        if type(node) is Node:
            stack.append(node)
        else:
            for index, key in enumerate(node.keys):
                results.append((key, node.values[index]))

        while stack:
            node = stack.pop()

            if type(node) is Node:
                for child in node.values:
                    stack.append(child)
            else:
                for index, key in enumerate(node.keys):
                    results.append((key, node.values[index]))

        return results

    def values(self):
        node = self.root
        stack = deque()

        results = []

        if type(node) is Node:
            stack.append(node)
        else:
            for index, key in enumerate(node.keys):
                results.append(node.values[index])

        while stack:
            node = stack.pop()

            if type(node) is Node:
                for child in node.values:
                    stack.append(child)
            else:
                for index, key in enumerate(node.keys):
                    results.append(node.values[index])

        return results

    def leftmost_leaf(self) -> Leaf:
        node = self.root
        while type(node) is not Leaf:
            node = node.values[0]
        return node
    
    
def demo():
    bplustree = BPlusTree()
    random.seed(1234567890)
    keys = random.sample(range(-1000000, 1000001), 10000)
    values = random.sample(range(-1000000, 1000001), 10000)

    for index, key in enumerate(keys):
        bplustree[key] = values[index]

    range_keys = []
    for index, key in enumerate(keys):
        if 1 <= key <= 1000001:
            range_keys.append(key)

    if sorted(bplustree.range_query(1, 1000001)[0]) != sorted(range_keys):
        print('something is wrong! 0')

    for index, key in enumerate(keys):
        if bplustree[key] != values[index] or bplustree.query(key) != values[index]:
            print('something is wrong! 1')

    updated_value = random.sample(range(-1000000, 1000001), 10000)
    for index, key in enumerate(keys):
        bplustree[key] = updated_value[index]

    for index, key in enumerate(keys):
        if bplustree[key] != updated_value[index] or bplustree.query(key) != updated_value[index]:
            print('something is wrong! 2')

    deleted_keys = random.sample(keys, 100)
    for index, key in enumerate(deleted_keys):
        bplustree.delete(key)
        if bplustree.query(key) != None:
            print('something is wrong! 3')

    range_keys = []
    for index, key in enumerate(keys):
        if key not in deleted_keys and 1 <= key <= 1000001:
            range_keys.append(key)

    if sorted(bplustree.range_query(1, 1000001)[0]) != sorted(range_keys):
        print('something is wrong! 3')

    keys2 = random.sample(range(2000000, 3000001), 10000)
    values2 = random.sample(range(2000000, 3000001), 10000)

    for index, key in enumerate(keys2):
        bplustree[key] = values2[index]

    for index, key in enumerate(keys2):
        if bplustree[key] != values2[index] or bplustree.query(key) != values2[index]:
            print('something is wrong! 4')

    range_keys = []
    for index, key in enumerate(keys):
        if key not in deleted_keys and 500000 <= key <= 2000001:
            range_keys.append(key)

    if sorted(bplustree.range_query(500000, 2000001)[0]) != sorted(range_keys):
        print('something is wrong! 4')

    for index, key in enumerate(keys):
        if key in deleted_keys:
            if bplustree.query(key) != None:
                print('something is wrong! 5')
        else:
            if bplustree[key] != updated_value[index] or bplustree.query(key) != updated_value[index]:
                print('something is wrong! 6')

    updated_value2 = random.sample(range(-1000000, 1000001), 10000)
    for index, key in enumerate(keys2):
        bplustree[key] = updated_value2[index]

    for index, key in enumerate(keys2):
        if bplustree[key] != updated_value2[index] or bplustree.query(key) != updated_value2[index]:
            print('something is wrong! 7')

    range_keys = []
    for index, key in enumerate(keys):
        if key not in deleted_keys and -500000 <= key <= 1500000:
            range_keys.append(key)

    if sorted(bplustree.range_query(-500000, 1500000)[0]) != sorted(range_keys):
        print('something is wrong! 8')

    print(bplustree[437537])
    print(bplustree.list_all())

if __name__ == '__main__':
    demo()
    