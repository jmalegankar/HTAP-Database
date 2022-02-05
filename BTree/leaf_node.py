import sys
from btree import BTree
from internal_node import InternalNode
from b_tree_node import BTreeNode
from leaf_node import LeafNode


class LeafNode:
    def __init__(self, lSize, p = None, l = None, r = None):
            self.internalNode = p
            self.leftTNode = l
            self.rightTNode = r
            self.bTNode = BTreeNode(lSize, p, self.leftTNode, self.rightTNode)
            self.values = []
    
    def addToLeft(self, value, last):
        self.bTNode.left = self.values[0]
        for i in range(0, (self.bTNode).count-1):
            self.values[i] = self.values[i+1]
        self.values[(self.bTNode).count-1] = last;
        if self.bTNode.parent: 
            self.bTNode.parent.resetMinimum(self.bTNode.parent)

    def addToRight(self, value, last):
        self.bTNode.right = last
        if (value == self.values[0] and (self.bTNode.parent)):
            self.bTNode.parent.resetMinimum(self.bTNode.parent)
      
    def addToThis(self, value):
        for i in range( (self.bTNode).count - 1, 0 , -1): 
            if self.values[i] > value:
                self.values[i+1] = self.values[i]
        
        self.values[i+1] = value;
        self.bTNode.count = self.bTNode.count + 1
        if (value == self.values[0] and self.bTNode.parent):
            self.bTNode.parent.resetMinimum(self.bTNode.parent)

    def addValue(self, value, last):
        if value > self.values[(self.bTNode).count-1]:
            last = value;
        else:
            last = self.values[(self.bTNode).count-1]
        for i in range((self.bTNode).count - 2, 0 , -1):
            if self.values[i] <= value:
                break
            self.values[i+1] = self.values[i]
        self.values[i+1] = value

    def getMaximum(self):
        if (self.bTNode).count > 0:
            return self.values[ (self.bTNode).count - 1 ]
        else:
            return sys.maxsize

    def getMinimum(self):
        if (self.bTNode).count > 0:
            return self.values[0]
        else:
            return 0

    def insert(self, value):
        last = 0;
        if ((self.bTNode).count < self.bTNode.leafSize):
            self.addToThis(value)
            return None

        self.addValue(value, last)
        
        if(self.bTNode.left and (self.bTNode.left.count < self.bTNode.leafSize)): #not sure what comes after "and"
            self.addToLeft(value, last)
            return None
        elif (self.bTNode.right and (self.bTNode.right.count < self.bTNode.leafSize)): #not sure what comes after "and"
            self.addToRight(value, last)
            return None
        else:
            return self.split(value, last) #not sure what split is 

    def remove(self, value):
        pos = 0;
        for pos in range(0, self.bTNode.count): #I don't understand this line
            if self.values[pos] == value:
                break
            if (pos < self.bTNode.count):
                self.bTNode.count -= 1
        for i in range(pos, self.bTNode.count):
            self.values[i] = self.values[i+1]
    
        if self.bTNode.count < (((self.bTNode).leafSize + 1) / 2):
            if self.bTNode.left:
                return self.removeWithLeftSibling()
            elif self.bTNode.right:
                return self.removewithRightSibling(pos)
    
        if pos == 0 and self.bTNode.parent:
            self.bTNode.parent = ((self.bTNode).parent).resetMinimum(self.bTNode.parent)
    
        return None


    def removeWithLeftSibling(self):
        if (self.bTNode.left.count > (self.bTNode.leafSize +1)/2):
            self.insert( self.bTNode.left.getMaximum())
            self.bTNode.left.remove(self.values[0]) #not sure 
        if (self.bTNode.parent):
            self.bTNode.parent.resetMinimum(self.bTNode.parent)
            return None
        else:
            for i in range(0, self.bTNode.count):
                self.bTNode.left.insert(self.values[i]) #not sure 
                self.bTNode.left.setRightSibling(self.bTNode.right) # unsure
            if (self.bTNode.right):
                self.bTNode.right.setLeftSibling(self.bTNode.left)
        return self #I think self.bTNode.right is 'this' in this case but I'm not sure

    def removeWithRightSibling(self, pos):
        if(self.bTNode.right.count > ((self.bTNode.leafSize) +1 )/ 2):
            self.insert(self.bTNode.right.getMinimum() ) #this is definitely wrong
            self.bTNode.right.remove(self.values[(self.bTNode).count - 1])
        if pos == 0:
            self.bTNode.parent.resetMinimum(self.bTNode.parent)
            return None
        else:
            for i in range(0, self.bTNode.count):
                self.bTNode.right.insert(self.values[i])
                self.bTNode.right = self.bTNode.setLeftSibling(self.bTNode.left)
            if (self.bTNode.left):
                self.bTNode.left = self.bTNode.setRigthSibling(self.bTNode.right)
            return self.bTNode.left




    def split (self, value, last):
        newPtr =  LeafNode(self.bTNode.leafsize, self.bTNode.parent, self, self.bTNode.right)
        
        if (self.bTNode.right):
            self.bTNode.right = self.bTNode.setLeftSibling(newPtr)
        self.bTNode.right = newPtr
        
        current = newPtr.bTNode.count
        for i in range(  ((((self.bTNode).leafSize) +1)/2), self.bTNode.leafSize):
            newPtr.values[current] = self.values[i] #idkk
            newPtr.bTNode.count += 1
            current = newPtr.bTNode.count
        newPtr.values[current] = last
        newPtr.bTNode.count += 1
        newPtr.bTNode.count = (self.bTNode.leafSize + 1) / 2
        
        if value == self.values[0] and self.bTNode.parent:
            self.bTNode.parent.resetMinimum(self)
        
        return newPtr

    def print_LeafNode(self, address_queue):
        print("Leaf: ")
        for i in range(0, self.bTNode.count):
            print(self.values[i] + ' ')
