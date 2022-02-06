import sys

class BTreeNode:
  def __init__(self, lSize = None: int, p = None: InternalNode, l = None : BTreeNode, r = None: BTreeNode):
        self.count = 0
        self.leafSize = lSize
        self.parent = InternalNode()
        self.leftSibling = BTreeNode()
        self.rightSibling = BTreeNode()
  
  # set parent
    def setParent(self, x):
        self.parent = x
  
  # set right sibling
    def setRightSibling(self, x):
        self.rightSibling = x
  
  #set left sibling
    def setLeftSibling(self, x):
        self.leftSibling = x


class InternalNode(BTreeNode):
    def __init__(self, iSize = None : int, lSize = None: int, p = None: InternalNode, l = None : BTreeNode, r = None: BTreeNode):
        self.internalSize = iSize
        self.keys = []
        self.children = []


    def addPtr(self, ptr: BTreeNode, pos: int):
        if (pos == self.internalSize):
            return ptr
        last = BTreeNode(self.children[self.count - 1])
    
        for i in range(self.count - 2, pos, -1):
            self.children[i + 1] = self.children[i]
            self.keys[i + 1] = self.keys[i]
        
        self.children[pos] = ptr
        self.keys[pos] = ptr.getMinimum()
        ptr.setParent(self)
        return last


    def addToLeft(self, last: BTreeNode):
        self.leftSibling.insert(children[0])
        for i in range(0, self.count-1):
            self.children[i] = self.children[i+1]
            self.keys[i] = self.keys[i+1]

        self.children[self.count -1] = last
        self.keys[self.count-1] = last.getMinimum() 
        last.setParent(self)
        if (self.parent):
            self.parent.resetMinimum(self)

    def addToRight(self, ptr: BTreeNode, last:BTreeNode):
        rightSibling.insert(last)
        if(ptr == self.children[0] and self.parent):
            self.parent.resetMinimum(self)

    def addToThis(self, ptr: BTreeNode, pos: int):
        for i in range(self.count-1, pos, -1):
            self.children[i+1] = self.children[i]
            self.keys[i+1] = self.keys[i]
        self.children[pos] = ptr
        self.keys[pos] = ptr.getMinimum()
        self.count += 1
        ptr.setParent(self)
        if (pos==0 and self.parent):
            self.parent.resetMinimum(self)

    def getMaximum(self): 
        if (self.count > 0):
            return self.children[self.count-1].getMaximum()

    def getMinimum(self):
        if (self.count > 0):
            return self.children[0].getMinimum()


<<<<<<< HEAD
    def insert(self, **kwargs): #there are 3 insert functions???
        if 'value' in kwargs:
            kvalue = kwargs.get('value')
            pos = 0
            if self.keys[pos] > kvalue:
                for pos in range(self.bTNode.count - 1, 0, -1):
                    if self.keys[pos] > kvalue:
                        continue
                    else:
                        break
            last = self.children[pos].insert(kvalue)
            ptr = self.children[pos].insert(kvalue)
            
            if not ptr :
                return None

            if self.bTNode.count < self.internalSize:
                self.addToThis(ptr, pos + 1)
                return None;
            
            last = self.addPtr(ptr, pos + 1)

            if self.bTNode.left and self.bTNode.left.count < self.internalSize:
                self.addToLeft(last)
                return None
            elif self.rightSibling and self.bTNode.right.count < self.internalSize:
                self.addToRight(ptr, last)
                return None
            else:
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
            knewNode = kwargs.get('newNode')
            pos = 0
    
            if knewNode.getMinimum() <= self.keys[0]:
                pos = 0
            else:
                pos = self.count
        
            self.addToThis(knewNode, pos)
=======
    def insertValue(self, value): #there are 3 insert functions???
      pos = 0
      if self.keys[pos] > value:
          for pos in range(self.bTNode.count - 1, 0, -1):
              if self.keys[pos] > value:
                  continue
              else:
                  break
      last = self.children[pos].insert(value)
      ptr = self.children[pos].insert(value)
      
      if not ptr :
          return None

      if self.bTNode.count < self.internalSize:
          self.addToThis(ptr, pos + 1)
          return None;
      
      last = self.addPtr(ptr, pos + 1)

      if self.bTNode.left and self.bTNode.left.count < self.internalSize:
          self.addToLeft(last)
          return None
      elif self.bTNode.right and self.bTNode.right.count < self.internalSize:
          self.addToRight(ptr, last)
          return None
      else:
          return self.split(last)

    def insertOldRoot(self, oldRoot, node2):
      self.children[0] = oldRoot
      self.children[1] = node2
      self.keys[0] = oldRoot.getMinimum()
      self.keys[1] = node2.getMinimum()
      self.bTNode.count = 2

      self.children[0].setLeftSibling(None)
      self.children[0].setRightSibling(self.children[1])
      self.children[1].setLeftSibling(self.children[0])
      self.children[1].setRightSibling(None)

      oldRoot.setParent(self)
      node2.setParent(self)

    def insertNewNode(self, newNode):
      pos = 0

      if newNode.getMinimum() <= self.keys[0]:
          pos = 0
      else:
          pos = self.bTNnode.count
  
      self.addToThis(newNode, pos)
>>>>>>> 7cb561d93b585da023c2c835ee1ea70132d8671e



    def remove(self, value):
        pos, i = 0
        for pos in range(self.count-1, 0, -1):
            if self.keys[pos] > value:
                continue
            else:
                break
        ptr = BTreeNode(self.children[pos].remove(value))
        if ptr:
            ptr = None
            self.count -= 1
            for i in range(pos, self.count):
                self.children[i] = self.children[i+1]
                self.keys[i] = self.keys[i+1]
            if (self.count < ((self.internalSize +1)/2) or self.count == 1):
                if (self.leftSibling):
                    return self.removeWithLeftSibling()
                elif (self.rightSibling):
                    return self.removeWithRightSibling(pos)
                
            if (self.parent and pos ==0 and self.count > 0):
                self.parent.resetMinimum(self)
        if (self.count == 1 and self.parent == None):
            return self.children[0]
        return None

    def removeChild(self, position):
        ptr = BTreeNode(self.children[position]) 
        self.count -= 1

        for i in range (position, self.count):
            self.children[i] = self.children[i+1]
            self.keys[i] = self.keys[i+1]

        if (position == 0 and self.parent):
            self.parent.resetMinimum(self)
        return ptr

    def removeWithLeftSibling(self, position):
        if (self.leftSibling.count > ((self.internalSize + 1)/2) ):
            self.insert(self.leftSibling.removeChild(self.leftSibling.count - 1) )
            if (self.parent):
                self.parent.resetMinimum(self)
            return None;
        else:
            for i in range(0, self.count):
                self.leftSibling.insert(self.children[i])
            self.leftSibling.setRightSibling(self.rightSibling)
            if (self.rightSibling):
                self.rightSibling.setLeftSibling(self.leftSibling)
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
        for i in range (0, self.bTNode.count):
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
                    
        for i in range( (self.internalSize + 1) / 2, self.internalSize ):
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

# class LeafNode:
#     def __init__(self, lSize, p = None, l = None, r = None):
#             self.internalNode = p
#             self.leftTNode = l
#             self.rightTNode = r
#             self.bTNode = BTreeNode(lSize, p, self.leftTNode, self.rightTNode)
#             self.values = []
    
#     def addToLeft(self, value, last):
#         self.bTNode.left = self.values[0]
#         for i in range(0, (self.bTNode).count-1):
#             self.values[i] = self.values[i+1]
#         self.values[(self.bTNode).count-1] = last;
#         if self.bTNode.parent: 
#             self.bTNode.parent.resetMinimum(self.bTNode.parent)

#     def addToRight(self, value, last):
#         self.bTNode.right = last
#         if (value == self.values[0] and (self.bTNode.parent)):
#             self.bTNode.parent.resetMinimum(self.bTNode.parent)
      
#     def addToThis(self, value):
#         print(self.values[0])
#         if self.values[i] > value:
#           for i in range( (self.bTNode).count - 1, 0 , -1 ): 
#               if self.values[i] > value:
#                   self.values[i+1] = self.values[i]
        
#         self.values[i+1] = value;
#         self.bTNode.count = self.bTNode.count + 1
#         if (value == self.values[0] and self.bTNode.parent):
#             self.bTNode.parent.resetMinimum(self.bTNode.parent)

#     def addValue(self, value, last):
#         if value > self.values[(self.bTNode).count-1]:
#             last = value;
#         else:
#             last = self.values[(self.bTNode).count-1]
#         for i in range((self.bTNode).count - 2, 0 , -1):
#             if self.values[i] <= value:
#                 break
#             self.values[i+1] = self.values[i]
#         self.values[i+1] = value

#     def getMaximum(self):
#         if (self.bTNode).count > 0:
#             return self.values[ (self.bTNode).count - 1 ]
#         else:
#             return sys.maxsize

#     def getMinimum(self):
#         if (self.bTNode).count > 0:
#             return self.values[0]
#         else:
#             return 0

#     def insert(self, value):
#         last = 0;
#         if ((self.bTNode).count < self.bTNode.leafSize):
#             self.addToThis(value)
#             return None

#         self.addValue(value, last)
        
#         if(self.bTNode.left and (self.bTNode.left.count < self.bTNode.leafSize)): #not sure what comes after "and"
#             self.addToLeft(value, last)
#             return None
#         elif (self.bTNode.right and (self.bTNode.right.count < self.bTNode.leafSize)): #not sure what comes after "and"
#             self.addToRight(value, last)
#             return None
#         else:
#             return self.split(value, last) #not sure what split is 

#     def remove(self, value):
#         pos = 0;
#         for pos in range(0, self.bTNode.count): #I don't understand this line
#             if self.values[pos] == value:
#                 break
#             if (pos < self.bTNode.count):
#                 self.bTNode.count -= 1
#         for i in range(pos, self.bTNode.count):
#             self.values[i] = self.values[i+1]
    
#         if self.bTNode.count < (((self.bTNode).leafSize + 1) / 2):
#             if self.bTNode.left:
#                 return self.removeWithLeftSibling()
#             elif self.bTNode.right:
#                 return self.removewithRightSibling(pos)
    
#         if pos == 0 and self.bTNode.parent:
#             self.bTNode.parent = ((self.bTNode).parent).resetMinimum(self.bTNode.parent)
    
#         return None


#     def removeWithLeftSibling(self):
#         if (self.bTNode.left.count > (self.bTNode.leafSize +1)/2):
#             self.insert( self.bTNode.left.getMaximum())
#             self.bTNode.left.remove(self.values[0]) #not sure 
#         if (self.bTNode.parent):
#             self.bTNode.parent.resetMinimum(self.bTNode.parent)
#             return None
#         else:
#             for i in range(0, self.bTNode.count):
#                 self.bTNode.left.insert(self.values[i]) #not sure 
#                 self.bTNode.left.setRightSibling(self.bTNode.right) # unsure
#             if (self.bTNode.right):
#                 self.bTNode.right.setLeftSibling(self.bTNode.left)
#         return self #I think self.bTNode.right is 'this' in this case but I'm not sure

#     def removeWithRightSibling(self, pos):
#         if(self.bTNode.right.count > ((self.bTNode.leafSize) +1 )/ 2):
#             self.insert(self.bTNode.right.getMinimum() ) #this is definitely wrong
#             self.bTNode.right.remove(self.values[(self.bTNode).count - 1])
#         if pos == 0:
#             self.bTNode.parent.resetMinimum(self.bTNode.parent)
#             return None
#         else:
#             for i in range(0, self.bTNode.count):
#                 self.bTNode.right.insert(self.values[i])
#                 self.bTNode.right = self.bTNode.setLeftSibling(self.bTNode.left)
#             if (self.bTNode.left):
#                 self.bTNode.left = self.bTNode.setRigthSibling(self.bTNode.right)
#             return self.bTNode.left

#     def split (self, value, last):
#         newPtr =  LeafNode(self.bTNode.leafsize, self.bTNode.parent, self, self.bTNode.right)
        
#         if (self.bTNode.right):
#             self.bTNode.right = self.bTNode.setLeftSibling(newPtr)
#         self.bTNode.right = newPtr
        
#         current = newPtr.bTNode.count
#         for i in range(  ((((self.bTNode).leafSize) +1)/2), self.bTNode.leafSize):
#             newPtr.values[current] = self.values[i] #idkk
#             newPtr.bTNode.count += 1
#             current = newPtr.bTNode.count
#         newPtr.values[current] = last
#         newPtr.bTNode.count += 1
#         newPtr.bTNode.count = (self.bTNode.leafSize + 1) / 2
        
#         if value == self.values[0] and self.bTNode.parent:
#             self.bTNode.parent.resetMinimum(self)
        
#         return newPtr

#     def print_LeafNode(self, address_queue):
#         print("Leaf: ")
#         for i in range(0, self.bTNode.count):
#             print(self.values[i] + ' ')

# class BTree:
#     def __init__(self, iSize: int, lSize: int):
#         self.internalSize = iSize
#         self.leafSize = lSize
#         self.root = LeafNode(lSize)

#     def insert(self, value:int):
#         ptr = BTreeNode()
#         ptr = self.root.insert(value)
#         if (ptr):
#           iPtr = InternalNode(self.internalSize, self.leafSize)
#           iPtr.insert(self.root, ptr)
#           self.root = iPtr
  
#     def remove(self, value):
#         ptr = self.root.remove(value)
#         if (ptr):
#           self.root = None
#           self.root = ptr
#           self.root = self.root.setParent(None)
