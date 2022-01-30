# simple b tree 
# I got some of this  code/logicfrom a website
# most of it is edited for some simple tests that I need a index for
# def not the best implmentation so please feel free to 

# import time and test it against any other implmentation
# for now i am just using this for some tests


class BTreeNode:
  def __init__(self, leaf=False):
    self.leaf = leaf
    self.keys = []
    self.child = []

class MYBTree:
    """
    space: the number of items at a certain level

    """
    def __init__(self, space):
        self.root = BTreeNode(True)
        self.t = space

    """
    k is the item being entered with k
    example (key, item)

    """

    def insert(self, k):
        root = self.root
        if len(root.keys) == (2 * self.t) - 1:
            temp = BTreeNode()
            self.root = temp
            temp.child.insert(0, root)
            self.split_child(temp, 0)
            self.insert_space_available(temp, k)
        else:
            self.insert_space_available(root, k)


    """"
    helper function to insert should not be called by user
    """
    def insert_space_available(self, B_tree, k):
        i = len(B_tree.keys) - 1
        if B_tree.leaf:
            B_tree.keys.append((None, None))
            while i >= 0 and k[0] < B_tree.keys[i][0]:
                B_tree.keys[i + 1] = B_tree.keys[i]
                i -= 1
            B_tree.keys[i + 1] = k
        else:
            while i >= 0 and k[0] < B_tree.keys[i][0]:
                i -= 1
            i += 1
            if len(B_tree.child[i].keys) == (2 * self.t) - 1:
                self.split_child(B_tree, i)
                if k[0] > B_tree.keys[i][0]:
                    i += 1
            self.insert_space_available(B_tree.child[i], k)

    """
    Another helper to insert/ helps balances the tree
    Again should not be called by user
    """
    def split_child(self, x, i):
        t = self.t
        y = x.child[i]
        z = BTreeNode(y.leaf)
        x.child.insert(i + 1, z)
        x.keys.insert(i, y.keys[t - 1])
        z.keys = y.keys[t: (2 * t) - 1]
        y.keys = y.keys[0: t - 1]
        if not y.leaf:
            z.child = y.child[t: 2 * t]
            y.child = y.child[0: t - 1]


    """
    call by a query most likely searches for a key
    call example
    Treevariable.search_key(key/id)
    """
    def search_key(self, k, x=None):
        if x is not None:
            i = 0
            while i < len(x.keys) and k > x.keys[i][0]:
                i += 1
            if i < len(x.keys) and k == x.keys[i][0]:
                return x.keys[i][1]
            elif x.leaf:
                return None
            else:
                return self.search_key(k, x.child[i])
        
        else:
            return self.search_key(k, self.root)

