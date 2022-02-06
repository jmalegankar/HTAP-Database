import sys
from btree import BTree
from internal_node import InternalNode
from b_tree_node import BTreeNode
from leaf_node import LeafNode

class BTreeNode:
	def __init__(self, lSize: int , p: InternalNode, l: BTreeNode, r: BTreeNode):
            self.count = 0
            self.leafSize = lSize
            self.parent = p
            self.left = l
            self.right = r

	    # get count
    def getCount(self):
        return self.count
    
    # get left sibling
    def getLeftSibling(self):
        return self.left
    
    # get right sibling
    def getRightSibling(self):
        return self.right
    
    # set parent
    def setParent(self, x):
        self.parent = x
    
    # set right sibling
    def setRightSibling(self, x):
        self.right = x
    
    #set left sibling
    def setLeftSibling(self, x):
        self.left = x
