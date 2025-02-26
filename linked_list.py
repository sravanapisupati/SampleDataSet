class Node:
    def __init__(self, data):
        self.data = data
        self.next = None

class LinkedList:
    def __init__(self, data):
        new_node = Node(data)
        self.head = new_node
        self.tail = new_node
        self.length = 1

    def display(self):
        current = self.head
        while current:
            print(current.data, end = " -> ")
            current = current.next
        print("None")
    
    def prepend(self, data):
        new_node = Node(data)
        if self.length == 0:
            self.head = new_node
            self.tail = new_node
        else:
            new_node.next = self.head
            self.head = new_node
        self.length += 1
        return True
    
    def append(self, data):
        new_node = Node(data)
        if self.length == 0:
            self.head = new_node
            self.tail = new_node
        else:
            self.tail.next = new_node
            self.tail = new_node
        self.length += 1
    
    def pop_first(self):
        if not self.head:
            return None
        temp = self.head
        self.head = self.head.next
        temp.next = None
        self.length -= 1
        if self.length == 0:
            self.tail = None
        return temp
    
    def pop(self):
        if self.length == 0:
            return None
        temp = self.head
        pre = self.head
        while(temp.next):
            pre = temp
            temp = temp.next
        self.tail = pre
        self.tail.next = None
        self.length -= 1
        if self.length == 0:
            self.head = None
            self.tail = None
        return temp
    
    def get(self, index):
        if index < 0 or index > self.length:
           return None
        current = self.head
        for _ in range(index):
            current = current.next
        return current
    
    def set_value(self, index, data):
        current = self.get(index)
        if current:
            current.data = data
            return True
        return False
    
    def remove(self, index):
        if index < 0 or index >= self.length :
            return None
        if index == 0:
            self.pop_first()
        if index == self.length - 1: 
            self.pop()
        prev = self.get(index - 1)
        current = prev.next
        prev.next = current.next
        current.next = None
        self.length -= 1
        return current
        
  
    def reverse(self):
        temp = self.head
        self.head = self.tail
        self.tail = temp
        after = temp.next
        before = None
        for _ in range(self.length):
            after = temp.next
            temp.next = before
            before = temp
            temp = after
        

l1_1 = LinkedList(10)
l1_1.append(20)

l1_1.append(30)
l1_1.append(40)
l1_1.append(50)

l1_1.display()
l1_1.reverse()
l1_1.display()  
