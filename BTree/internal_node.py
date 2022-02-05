import sys
from BTree.b_tree_node import BTreeNode

class InternalNode:
	def __init__(iSize, lSize, p, l, r):
      	self.internalSize = iSize
        self.bTNode = BTreeNode(lSize, p, l, r)
        self.keys = []
        self.children = []


    def addPtr(self, ptr, pos):
        if (pos == self.internalSize):
            return ptr
      
        last = self.children[self.bTNode.count - 1]
      
        for i in range(pos, self.bTNode.count - 2, -1)
            self.children[i + 1] = self.children[i]
            self.keys[i + 1] = self.keys[i]
        
        self.children[pos] = ptr
        self.keys[pos] = ptr.getMinimum()
        ptr.setParent(self)
        return last


    def addToLeft(self, last):
        for i in range(0, self.bTNode.count-1):
            self.children[i] = self.children[i+1]
            self.keys[i] = self.keys[i+1]
        self.children[self.bTNode.count -1] = last
        self.keys[self.bTNode.count-1] = last.getMinimum() 
        last.setParent(self)
        if (self.bTNode.parent):
            self.bTNode.parent.resetMinimum(self)

    def addToRight(self, ptr, last):
        if(ptr == self.children[0] and self.bTNode.parent):
            self.bTNode.parent.resetMinimum(self)

    def addToThis(self, ptr, pos):
        for i in range(self.bTNode.count-1, pos, -1):
            self.children[i+1] = self.children[i]
        self.keys[i+1] = self.keys[i]
        self.children[pos] = ptr
        self.keys[pos] = ptr.getMinimum()
        self.bTNode.count += 1
        ptr.setParent(self)
        if (pos==0 and self.bTNode.parent):
            self.bTNode.parent.resetMinimum(self)

    def getMaximum(self): 
        if (self.bTNode.count > 0):
            return self.children[self.bTNode.count-1] = getMaximum()
        else:
            return sys.INT_MAX

    def getMinimum(self):
        if (self.bTNode.count > 0):
            return self.children[0] = getMinimum()
        else:
            return 0


    def insert(self, **kwargs): #there are 3 insert functions???
        if 'value' in kwargs:
            kvalue = kwargs.get('value')
            pos = 0
            if self.keys[pos] > kvalue:
                for pos in range(self.bTNode.count - 1, 0, -1)
                    if self.keys[pos] > kvalue:
                        continue
                    else:
                        break
            last = self.children[pos].insert(kvalue)
            ptr = self.children[pos].insert(kvalue)
            
            if not ptr :
                return None

            if self.bTNode.count < self.internalSize
                self.addToThis(ptr, pos + 1)
                return None;
            
            last = addPtr(ptr, pos + 1)

            if self.bTNode.left and self.bTNode.left.count < self.internalSize
                self.addToLeft(last)
                return None
            elif self.bTNode.right and self.bTNode.right.count < self.internalSize
                self.addToRight(ptr, last)
                return None
            else
                return self.split(last)

        if 'oldRoot' in kwargs:
            koldRoot = kwargs.get('oldRoot')
            knode2 = kwargs.get('node2')
            self.children[0] = koldRoot
            self.children[1] = knode2
            self.keys[0] = koldRoot.getMinimum()
            self.keys[1] = knode2.getMinimum()
            self.bTNode.count = 2

            self.children[0].setLeftSibling(None)
            self.children[0].setRightSibling(self.children[1])
            self.children[1].setLeftSibling(self.children[0])
            self.children[1].setRightSibling(None)

            koldRoot.setParent(self)
            knode2.setParent(self)
        if 'newNode' in kwargs:
            knewRoot = kwargs.get('newNode')
            pos = 0
      
            if knewNode.getMinimum() <= self.keys[0]:
                pos = 0
            else:
                pos = self.bTNnode.count
        
            self.addToThis(knewNode, pos)



    def remove(self, value):
        pos, i = 0
        for pos in range(self.bTNode.count-1, 0, -1)
            if self.keys[pos] > value:
                continue
            else:
                break
            ptr = self.children[pos].remove(value) #??
            if ptr:
                ptr = None
                self.bTNode.count -= 1
                for i in range(pos, count):
                    self.children[i] = children[i+1]
                    self.keys[i] = self.keys[i+1]
                if (self.bTNode.count < ((self.internalSize +1)/2) or self.bTNode.count == 1):
                    if (self.bTNode.left):
                        return self.removeWithLeftSibling()
                    elif (self.bTNode.right):
                        return self.removeWithRightSibling(pos)
                    
                if (self.bTNode.parent and pos ==0 and self.bTNode.count > 0):
                    self.bTNode.parent.resetMinimum(self)
            if (self.bTNode.count ==1 and self.bTNode.parent ==None):
                return self.children[0]
        return None

    def removeChild(self, position):
        ptr = self.children[position]
        self.bTNode.count -= 1
        for i in range (position, self.bTNode.count):
            self.children[i] = self.children[i+1]
            self.keys[i] = self.keys[i+1]

        if (position == 0 and self.bTNode.parent):
            self.bTNode.parent.resetMinimum(self)
        return ptr

    def removeWithLeftSibling(self, position):
        if (self.bTNode.left.count > ((self.internalSize + 1)/2) ):
            self.insert( self.bTNode.left.removeChild(self.bTNode.left.count - 1) )
            if (self.bTNode.parent):
                self.bTNode.parent.resetMinimum(self)
            return None;
        else:
            for i in range(0, count):
                self.bTNode.left.insert(self.children[i])
            self.bTNode.left.setRightSibling(self.bTNode.right)
            if (self.bTNode.right):
                self.bTNode.right.setLeftSibling(self.bTNode.left)
            return self

    def removeWithRightSibling(self, position):
        if (self.bTNode.right.count > (self.internalSize + 1)/2):
            self.insert(self.bTNode.right.removeChild(0))
            if (position == 0):
                self.bTNode.parent.resetMinimum(self)
            return None
        else:
            for i in range(self.bTNode.count-1, 0, -1):
                self.bTNode.right.insert(self.children[i])
            self.bTNode.right.setLeftSibling(self.bTNode.left)
            if (self.bTNode.left):
                self.bTNode.left.setRightSibling(self.bTNode.right)
            return self 

    def resetMinimum(self, child):
        for i in range (0, count):
            if self.children[i] == child:
                self.keys[i] = self.children[i].getMinimum()
                if (i == 0 and self.bTNode.parent):
                    self.bTNode.parent.resetMinimum(self) #??
            break

    def split(self, last):
        newPtr = InternalNode(self.internalSize, self.bTNode.leafSize, self.bTNode.parent, self, self.bTNode.right)
        if self.bTNode.right:
            self.bTNode.right.setLeftSibling(newPtr)
                       
        self.bTNode.right = newPtr
                       
        for i in range( (self.internalSize + 1) / 2, self.internalSize )
            newPtr.children[newPtr.bTNode.count] = self.children[i] 
            newPtr.keys[newPtr.bTNode.count] = self.keys[i]
            newPtr.bTNode.count += 1
            self.children[i].setParent(newPtr) 
        newPtr.children[newPtr.bTNode.count] = last
        newPtr.keys[newPtr.bTNode.count] = last.getMinimum()
        newPtr.bTNode.count += 1
        last.setParent(newPtr)
        self.bTNode.count = (self.internalSize + 1) /2
        return newPtr


