package datastructures.projects.database;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;

public class BTree<Key extends Comparable<Key>, Value> implements Serializable {

    private static final int max = 4;
    private int height;
    private int count;
    private Node root;


    private static class Node implements Serializable {
        private Entry[] entries = new Entry[max];
        private int entryCount;

        public Node(int entryCount) {
            this.entryCount = entryCount;
        }
    }


    private static class Entry implements Serializable{
        private Comparable key;
        private Object value;
        private Node child;

        public Entry(Comparable key, Object value, Node child) {
            this.key = key;
            this.value = value;
            this.child = child;
        }

        public Comparable getKey() {
            return this.key;
        }

        public Object getValue() {
            return this.value;
        }

        public Node getChild() {
            return this.child;
        }
    }


    public BTree() {
        this.root = new Node(0);

    }



    public int bTreeSize() {
        return this.count;
    }

    public boolean isEmpty() {
        return bTreeSize() == 0;
    }

    public int bTreeHeight() {
        return this.height;
    }

    private static boolean less(Comparable inputKey, Comparable nodeKey) {
        return inputKey.compareTo(nodeKey) < 0;
    }

    private static boolean isEqual(Comparable inputKey, Comparable nodeKey) {
        return inputKey.compareTo(nodeKey) == 0;
    }

    private Node put(Node currentNode, Key key, Value val, int height) {
        int j;
        Entry newEntry = new Entry(key, val, null);
//external node
        if (height == 0) {
//find index in currentNode’s entry[] to insert new entry
            for (j = 0; j < currentNode.entryCount; j++) {
                if (less(key, currentNode.entries[j].key)) {
                    break;
                }
            }
        } else {
            for (j = 0; j < currentNode.entryCount; j++) {
//if (we are at the last key in this node OR the key we
//are looking for is less than the next key, i.e. the
//desired key must be added to the subtree below the current entry),
//then do a recursive call to put on the current entry’s child
                if ((j + 1 == currentNode.entryCount) || less(key, currentNode.entries[j + 1].key)) {
//increment j (j++) after the call so that a new entry created by a split
//will be inserted in the next slot
                    Node newNode = this.put(currentNode.entries[j++].child,
                            key, val, height - 1);
                    if (newNode == null) {
                        return null;
                    }
//if the call to put returned a node, it means I need to add a new entry to
//the current node
                    newEntry.key = newNode.entries[0].key;
                    newEntry.child = newNode;
                    break;
                }
            }
        }
        for (int i = currentNode.entryCount; i > j; i--) {
            currentNode.entries[i] = currentNode.entries[i - 1];
        }
//add new entry
        currentNode.entries[j] = newEntry;
        currentNode.entryCount++;
        if (currentNode.entryCount < BTree.max) {
//no structural changes needed in the tree
//so just return null
            return null;
        } else {
//will have to create new entry in the parent due
//to the split, so return the new node, which is
//the node for which the new entry will be created
            return this.split(currentNode);
        }
    }

    public void put(Key key, Value val) {
        Node newNode = this.put(this.root, key, val, this.height);
        this.count++;
        if (newNode == null) {
            return;
        }
//split the root:
//Create a new node to be the root.
//Set the old root to be new root's first entry.
//Set the node returned from the call to put to be new root's second entry
        Node newRoot = new Node(2);
        newRoot.entries[0] = new Entry(this.root.entries[0].key, null, this.root);
        newRoot.entries[1] = new Entry(newNode.entries[0].key, null, newNode);
        this.root = newRoot;
//a split at the root always increases the tree height by 1
        this.height++;
    }

    private Node split(Node currentNode) {
        Node newNode = new Node(BTree.max / 2);
//by changing currentNode.entryCount, we will treat any value
//at index higher than the new currentNode.entryCount as if
//it doesn't exist
        currentNode.entryCount = BTree.max / 2;
//copy top half of h into t
        for (int j = 0; j < BTree.max / 2; j++) {
            newNode.entries[j] = currentNode.entries[BTree.max / 2 + j];
            currentNode.entries[BTree.max / 2 + j] = null;
        }
        return newNode;
    }

    public Value get(Key key)
    {
        return this.get(this.root, key, this.height);
    }
    private Value get(Node currentNode, Key key, int height)
    {
        Entry[] entries = currentNode.entries;
//current node is external (i.e. height == 0)
        if (height == 0)
        {
            for (int j = 0; j < currentNode.entryCount; j++)
            {
                if(isEqual((Comparable) key, (Comparable) entries[j].key))
                {
//found desired key. Return its value
                    return (Value)entries[j].value;
                }
            }
//didn't find the key
            return null;
        }
//current node is internal (height > 0)

        else
        {
            for (int j = 0; j < currentNode.entryCount; j++)
            {
//if (we are at the last key in this node OR the key we
//are looking for is less than the next key, i.e. the
//desired key must be in the subtree below the current entry),
//then recurse into the current entry’s child
                if (j + 1 == currentNode.entryCount || less(key, entries[j + 1].key))
                {
                    return this.get(entries[j].child, key, height - 1);
                }
            }
//didn't find the key
            return null;
        }
    }

    

    public static void main(String[] args) {
        BTree<Integer, String> st = new BTree<Integer, String>();
        st.put(1, "one");
        st.put(2, "two");
        st.put(3, "three");
        st.put(4, "four");
        st.put(5, "five");
        st.put(5, "six");
        st.put(5, null);
        st.put(8, "eight");
        st.put(9, "nine");
        st.put(10, "ten");
        st.put(11, "eleven");
        st.put(12, "twelve");
        st.put(13, "thirteen");
        st.put(14, "fourteen");
        st.put(15, "fifteen");
        st.put(16, "sixteen");
        st.put(17, "seventeen");
        st.put(18, "eighteen");
        st.put(19, "nineteen");
        st.put(20, "twenty");
        st.put(21, "twenty one");
        st.put(22, "twenty two");
        st.put(23, "twenty three");
        st.put(24, "twenty four");
        st.put(25, "twenty five");
        st.put(26, "twenty six");
        st.put(27, "twenty seven");
        st.put(28, "twenty eight");
        st.put(29, "twenty nine");
        st.put(30, "thirty");
        st.put(31, "thirty one");
        st.put(32, "thirty two");
        st.put(0, "Xero");
        st.put(0, null);
        //ArrayList list = st.get(3);
        for(int i = 0; i < 33; i++) {
            System.out.println(st.get(i));
        }


    }
}


