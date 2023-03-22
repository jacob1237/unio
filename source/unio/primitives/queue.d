module unio.primitives.queue;

import core.lifetime : copyEmplace;
import std.range.primitives : isInputRange, isOutputRange, hasLength;
import std.range.interfaces : InputRange, OutputRange;
import std.typecons : NullableRef;

@safe @nogc:

public enum isQueue(R) =
    isInputRange!R &&
    isOutputRange!R &&
    hasLength!R &&
    is(R.full == bool) &&
    is(R.capacity == size_t) &&
    is(R.reset == void);

/** 
 * This implementation is an atomic lock-free single producer / single consumer ring buffer.
 *
 * The lock-freeiness is achieved by one additional array cell at the end of the buffer,
 * so every time you define the size of the buffer, the internal array lenghth will always be size + 1.
 *
 * Thus, each thread is writing to its own variables (head/tail) without contention.
 *
 * If your ring buffer entry size is huge, you should not use this implementation.
 *
 * TODO: Allocate memory on the heap rather than on stack
 */
public struct RingBuffer(E, size_t Size)
{
pure nothrow:
    enum Capacity = Size + 1;

private:
    E[Capacity] data;
    size_t head = 0, tail = 0;
    size_t next(in size_t val) const { return (val + 1) % Capacity; }

public:
    @property
    {
        bool empty() const { return tail == head; }
        bool full() const { return next(tail) == head; }
        immutable(E) front() const { return data[head]; }
        size_t capacity() const { return Size; }
        size_t length() const { return tail - head; }
    }

    /** 
     * Reset queue buffer length to zero
     */
    void reset()
    {
        tail = head;
    }

    void popFront()
    {
        if (!empty) {
            head = next(head);
        }
    }

    void put(E elem) @trusted
    {
        immutable nextTail = next(tail);

        if (nextTail != head) {
            copyEmplace(elem, data[tail]);
            tail = nextTail;
        }
    }
}

/** 
The BinaryHeap implements an array-based binary heap (min or max is defined by the user).

This implementation also provides some convenient hooks that help maintain connection
between user data, nodes and their indexes in the array (`compareAt()` and `swapAt()`).

The reason for implementing it that way is because the heap must allow to remove
arbitrary elements without searching them in the array. This requirement comes from
the ability to cancel ongoing operations scheduled in the queue (timeouts in particular).

Caveats:
  - No checks for duplicate items
  - Not a standalone generic type: neither acquires nor provides ref counting for the store

TODO: Auto-shrink the container array after certain removal threshold
*/
public template BinaryHeap(Store)
{
    // Element type of the Store
    alias T = typeof(Store.front);

    private:
        size_t siftUp(ref Store store, size_t idx)
        {
            size_t child = idx;

            for (size_t parent; child; child = parent)
            {
                parent = (child - 1) / 2;
                if (!store.compareAt(parent, child)) break;

                store.swapAt(parent, child);
            }

            return child;
        }

        size_t siftDown(ref Store store, size_t idx)
        {
            auto parent = idx;

            for(size_t child; ; parent = child)
            {
                child = (parent + 1) * 2;

                if (child >= store.length)
                {
                    // Leftover left node
                    if (child == store.length)
                    {
                        store.swapAt(parent, --child);
                        parent = child;
                    }

                    break;
                }

                const leftChild = child - 1;
                if (store.compareAt(child, leftChild)) child = leftChild;

                store.swapAt(parent, child);
            }

            return parent;
        }

    public:
        size_t insert(ref Store store, in T entry)
        {
            store.insertBack(entry);
            return siftUp(store, store.length - 1);
        }

        void remove(ref Store store, size_t idx)
        {
            if (store.empty || store.length <= idx) return;

            store.swapAt(idx, store.length - 1);
            store.removeBack();
            siftUp(store, siftDown(store, idx));
        }
}

@("heapTest")
unittest
{
    struct PriorityQueue(T, size_t Length)
    {
        import std.algorithm.mutation : swapAt;

        alias MinHeap = BinaryHeap!(typeof(this));

        private:
            T[Length] data;
            size_t head;

            int compareAt(size_t l, size_t r) { return data[l] > data[r]; }
            void swapAt(size_t l, size_t r) { data.swapAt(l, r); }
            void insertBack(T val) { data[head++] = val; }
            void removeBack() { head--; }

        public:
            @property auto front() { return data[0]; }
            @property bool empty() const { return !head; }
            @property auto length() const { return head; }
            void popFront() { MinHeap.remove(this, 0); }
            size_t put(in T val) { return MinHeap.insert(this, val); }
            void remove(size_t idx) { MinHeap.remove(this, idx); }
    }

    with (PriorityQueue!(int, 10)())
    {
        static immutable values = [20, 21, 1, 100, 35];

        foreach (val; values) put(val);
        assert(length == 5);
        assert(front == 1);

        popFront();
        assert(length == 4);
        assert(front == 20);

        remove(1);
        assert(length == 3);
        assert(front == 20);

        popFront();
        assert(length == 2);
        assert(front == 35);

        popFront();
        assert(length == 1);
        assert(front == 100);

        popFront();
        assert(empty);
    }
}

/** 
The Queue implements a FIFO queue on top of an external storage. It doesn't retain any data
itself, except the `tail` and `head` pointers, but from the interface viewpoint,
it stores node IDs/keys in a specific order, being compatible with the built-in Range interface.

The `Resolver` delegate is required for obtaining data from the storage.

To be compatible with the queue interface, the storage entry must have the `prev` and `next` fields
in its body.

Time complexity:
    - Insert: O(1)
    - Remove: O(1)
*/
public struct Queue(K)
{
    alias Resolver = NullableRef!Node delegate (K) @nogc;

    struct Node
    {
        K prev;
        K next;
    }

    private:
        K head;
        K tail;
        size_t _length;
        Resolver resolve;

    public:
        @disable this();
        @disable this(this);

        this(Resolver r)
        {
            resolve = r;
        }

        @property
        {
            size_t length() const { return _length; }
            bool empty() const { return !length; }
            inout(K) front() inout { return head; }
        }

        void put(K entry)
        {
            with (resolve(entry))
            {
                if (isNull) assert(false, "Can't insert a non-existing entry into the Queue");

                auto tailNode = resolve(tail);
                if (!tailNode.isNull) { tailNode.next = entry; }
                else { head = entry; }

                prev = tail;
                next = K.init;

                tail = entry;
                _length++;
            }
        }

        void remove(in Node node)
        {
            with (node)
            {
                auto prevNode = resolve(prev);
                if (prevNode.isNull) { head = next; }
                else { prevNode.next = next; }

                auto nextNode = resolve(next);
                if (nextNode.isNull) { tail = prev; }
                else { nextNode.prev = prev; }

                _length--;
            }
        }

        void popFront()
        {
            auto node = resolve(head);
            if (node.isNull) assert(false, "Can't pop a non-existing entry from the Queue");

            remove(node);
        }
}

@("queuePutRemove")
unittest
{
    alias Key = size_t;
    alias Node = Queue!(Key).Node;

    struct Entry
    {
        size_t id;
        Node node;
    }

    Entry[3] data = [Entry(1), Entry(2), Entry(3)];

    auto resolve(Key k)
    {
        const idx = k - 1;
        return NullableRef!Node(idx <= data.length ? &data[idx].node : null);
    }

    import std.functional : toDelegate;
    auto q = Queue!(Key)(toDelegate(&resolve));

    assert(q.empty);
    assert(q.length == 0);

    q.put(1);
    q.put(2);
    q.put(3);
    with (resolve(1)) assert(prev == 0 && next == 2);
    with (resolve(2)) assert(prev == 1 && next == 3);
    with (resolve(3)) assert(prev == 2 && next == 0);
    assert(!q.empty);
    assert(q.length == 3);
    assert(q.front == 1);
    assert(q.head == 1);
    assert(q.tail == 3);

    q.remove(resolve(2));
    with (resolve(1)) assert(prev == 0 && next == 3);
    with (resolve(3)) assert(prev == 1 && next == 0);
    assert(!q.empty);
    assert(q.length == 2);
    assert(q.front == 1);
    assert(q.head == 1);
    assert(q.tail == 3);

    q.remove(resolve(1));
    with (resolve(3)) assert(prev == 0 && next == 0);
    assert(!q.empty);
    assert(q.length == 1);
    assert(q.head == 3);
    assert(q.tail == 3);

    q.popFront();
    assert(q.empty);
    assert(q.length == 0);
    assert(q.head == 0);
    assert(q.tail == 0);
}
