# -*- coding: utf-8 -*-
from bisect import bisect

__all__ = ('BPTree', 'BPTreeNode',
   'BPTreeProvider', 'BPTreeSimpleProvider')

null = object ()
#------------------------------------------------------------------------------#
# B+Tree                                                                       #
#------------------------------------------------------------------------------#
class BPTree (object):
    def __init__ (self, provider):
        self.provider = provider

    def Get (self, key, default = null):
        # provider
        desc2node = self.provider.DescToNode

        # find leaf
        node = self.provider.Root ()
        for depth in range (self.provider.Depth () - 1):
            node = desc2node (node.children [bisect (node.keys, key)])

        # check key
        index = bisect (node.keys, key)
        if not index or node.keys [index - 1] != key:
            if default is null:
                raise KeyError (key)
            return default

        return node.children [index]

    def GetRange (self, low = None, high = None):
        # validate range
        if low is not None and high is not None and low >= high:
            return

        # find first leaf
        desc2node = self.provider.DescToNode
        node = self.provider.Root ()
        if low is not None:
            for depth in range (self.provider.Depth () - 1):
                node = desc2node (node.children [bisect (node.keys, low)])
            index = bisect (node.keys, low)
        else:
            for depth in range (self.provider.Depth () - 1):
                node = desc2node (node.children [0])
            index = 1

        # iterate over whole leafs
        while not high or node.keys [-1] < high:
            for index in range (index, len (node.keys) + 1):
                yield node.keys [index - 1], node.children [index]
            node = node.children [-1]
            if node is None:
                return
            index = 1

        # iterate over last leaf
        for index in range (index, bisect (node.keys, high) + 1):
            yield node.keys [index - 1], node.children [index]

    def Add (self, key, value):
        # provider
        order = self.provider.Order ()
        dirty = self.provider.Dirty
        desc2node = self.provider.DescToNode
        node2desc = self.provider.NodeToDesc

        # find path
        node, path = self.provider.Root (), []
        for depth in range (self.provider.Depth () - 1):
            index = bisect (node.keys, key)
            path.append ((index, node))
            node = desc2node (node.children [index])

        # check if value is updated
        index = bisect (node.keys, key)
        if index and key == node.keys [index - 1]:
            node.children [index] = value
            dirty (node)
            return
        path.append ((index, node))

        # size += 1
        self.provider.Size (self.provider.Size () + 1)

        # update tree
        sibling = None
        while path:
            index, node = path.pop ()

            # add new key
            node.keys.insert (index, key)
            node.children.insert (index + 1, value)
            dirty (node)

            if len (node.keys) < order:
                return

            # node is full so we need to split it
            center = len (node.children) >> 1
            if node.is_leaf:
                # create sibling
                sibling = self.provider.NodeCreate (node.keys [center - 1:], node.children [center:], True)
                node.keys, node.children = node.keys [:center - 1], node.children [:center]

                # keep leafs linked
                node.children.append (node2desc (sibling))
                sibling.children.insert (0, node2desc (node))

                # update key
                key = sibling.keys [0]

            else:
                # create sibling
                sibling = self.provider.NodeCreate (node.keys [center:], node.children [center:], False)
                node.keys, node.children = node.keys [:center], node.children [:center]

                # update key
                key = node.keys.pop ()

            dirty (sibling)
            value = node2desc (sibling)

        # create new root
        self.provider.Depth (self.provider.Depth () + 1) # depth += 1
        self.provider.Root (self.provider.NodeCreate ([key],
            [node2desc (self.provider.Root ()), node2desc (sibling)], False))

    def Pop (self, key, default = null):
        # provider
        half_order = self.provider.Order () >> 1
        dirty = self.provider.Dirty
        desc2node = self.provider.DescToNode

        # find path
        node, path = self.provider.Root (), []
        for depth in range (self.provider.Depth () - 1):
            index = bisect (node.keys, key)
            parent, node = node, desc2node (node.children [index])
            path.append ((node, index, parent))

        # check if key exists
        index = bisect (node.keys, key)
        if not index and node.keys [index - 1] != key:
            if default is null:
                raise KeyError (key)
            return default
        value = node.children [index]

        # size -= 1
        self.provider.Size (self.provider.Size () - 1)

        # update tree
        while path:
            node, node_index, parent = path.pop ()

            # remove scheduled (key | child)
            del node.keys [index - 1]
            del node.children [index]

            if len (node.keys) >= half_order:
                dirty (node)
                return value

            #------------------------------------------------------------------#
            # Redistribute                                                     #
            #------------------------------------------------------------------#
            left, right = None, None
            if node_index > 0:
                # has left sibling
                left = desc2node (parent.children [node_index - 1])
                if len (left.keys) > half_order:
                    # borrow from left sibling
                    if node.is_leaf:
                        # copy left key
                        parent.keys [node_index - 1] = left.keys [-1]
                        # move left key to node
                        node.keys.insert (0, left.keys.pop ())
                        # move left child to node (don't forget about leaf links)
                        node.children.insert (1, left.children.pop (-2))
                    else:
                        # copy parent's key to node
                        node.keys.insert (0, parent.keys [node_index - 1])
                        # move left key to parent
                        parent.keys [node_index - 1] = left.keys.pop ()
                        # move left child to node
                        node.children.insert (0, left.children.pop ())

                    dirty (node), dirty (left), dirty (parent)
                    return value

            if node_index < len (parent.keys):
                # has right sibling
                right = desc2node (parent.children [node_index + 1])
                if len (right.keys) > half_order:
                    # borrow from right sibling
                    if node.is_leaf:
                        # copy right key
                        parent.keys [node_index] = right.keys [1]
                        # move right key to node
                        node.keys.append (right.keys.pop (0))
                        # move right child to node (don't forget about leaf links)
                        node.children.insert (-1, right.children.pop (1))
                    else:
                        # copy parent's key to node
                        node.keys.append (parent.keys [node_index])
                        # move right key to parent
                        parent.keys [node_index] = right.keys.pop (0)
                        # move right child to node
                        node.children.append (right.children.pop (0))

                    dirty (node), dirty (right), dirty (parent)
                    return value

            #------------------------------------------------------------------#
            # Merge                                                            #
            #------------------------------------------------------------------#
            src, dst, index = ((node, left, node_index) if left
                else (right, node, node_index + 1))

            if node.is_leaf:
                # remove dst from chain of leafs
                dst.children.pop ()
                dst_desc = src.children.pop (0)

                # update next if src isn't the last leaf
                src_next_desc = src.children [-1]
                if src_next_desc is not None:
                    src_next = desc2node (src_next_desc)
                    src_next.children [0] = dst_desc
                    dirty (src_next)
            else:
                # copy parents key
                dst.keys.append (parent.keys [index - 1])

            # copy node's (keys | children)
            dst.keys.extend (src.keys)
            dst.children.extend (src.children)

            # mark nodes
            self.provider.Release (src)
            dirty (dst)

        #----------------------------------------------------------------------#
        # Update Root                                                          #
        #----------------------------------------------------------------------#
        root = self.provider.Root ()
        del root.keys [index - 1]
        del root.children [index]

        if not root.keys:
            depth = self.provider.Depth ()
            if depth > 1:
                # root is not leaf because depth > 1
                self.provider.Root (desc2node (*root.children))
                self.provider.Release (root)
                self.provider.Depth (depth - 1) # depth -= 1
        else:
            dirty (root)

        return value

    #--------------------------------------------------------------------------#
    # Mutable Map Interface                                                    #
    #--------------------------------------------------------------------------#
    def __iter__ (self):
        pass

#------------------------------------------------------------------------------#
# B+Tree Node                                                                  #
#------------------------------------------------------------------------------#
class BPTreeNode (object):
    __slots__ = ('keys', 'children', 'is_leaf',)

    def __init__ (self, keys, children, is_leaf):
        self.keys, self.children, self.is_leaf = keys, children, is_leaf

#------------------------------------------------------------------------------#
# B+Tree Provider                                                              #
#------------------------------------------------------------------------------#
class BPTreeProvider (object):
    # transformation
    def NodeToDesc (self, node):
        raise NotImplementedError ()

    def DescToNode (self, desc):
        raise NotImplementedError ()

    # creation | deletion
    def Dirty (self, node):
        raise NotImplementedError ()

    def Release (self, node):
        raise NotImplementedError ()

    def NodeCreate (self, keys, children, is_leaf):
        raise NotImplementedError ()

    # properties
    def Size (self, value = None):
        raise NotImplementedError ()

    def Depth (self, value = None):
        raise NotImplementedError ()

    def Root (self, value = None):
        raise NotImplementedError ()

    def Order (self):
        pass

#------------------------------------------------------------------------------#
# B+Tree Simple Provider                                                       #
#------------------------------------------------------------------------------#
class BPTreeSimpleProvider (BPTreeProvider):
    def __init__ (self, order):
        self.root = self.NodeCreate ([], [None, None], True)
        self.size  = 0
        self.depth = 1
        self.order = order

    def NodeToDesc (self, node):
        return node

    def DescToNode (self, desc):
        return desc

    def Dirty (self, node):
        pass

    def Release (self, node):
        pass

    def NodeCreate (self, keys, children, is_leaf):
        return BPTreeNode (keys, children, is_leaf)

    def Size (self, value = None):
        self.size = self.size if value is None else value
        return self.size

    def Depth (self, value = None):
        self.depth = self.depth if value is None else value
        return self.depth

    def Root (self, value = None):
        self.root = self.root if value is None else value
        return self.root

    def Order (self):
        return self.order
# vim: nu ft=python columns=120 :
