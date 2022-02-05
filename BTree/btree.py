from BTree.leaf_node import LeafNode
from BTree.internal_node import InternalNode


class BTree:
  def __init__(iSize, lSize):
    self.internalSize = iSize
    self.leafSize = lSize
    self.root = LeafNode(lSize)

  def insert(self, value):
    ptr = self.root.insert(value)
    if (ptr):
      iPtr = InternalNode(self.internalSize, self.leafsize)
      iPtr.insert(self.root, ptr)
      self.root = iPtr
  
  def remove(self, value):
    ptr = self.root.remove(value)
    if (ptr):
      self.root = None
      self.root = ptr
      self.root = setParent(None)