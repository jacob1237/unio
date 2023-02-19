module unio.primitives.queue;

@safe @nogc:

import core.lifetime : copyEmplace;
import std.range.primitives : isInputRange, isOutputRange, hasLength;
import std.range.interfaces : InputRange, OutputRange;

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
public struct BinaryHeap(Store)
{
    import unio.primitives.allocator : makeArray, resizeArray;

    // Element type of the Store
    alias T = typeof(Store.front);

    private:
        size_t _length;
        Store* store;

        size_t siftUp(size_t idx)
        {
            auto child = idx;

            for (size_t parent; child; child = parent)
            {
                parent = (child - 1) / 2;
                if (!store.compareAt(parent, child)) break;

                store.swapAt(parent, child);
            }

            return child;
        }

        size_t siftDown(size_t idx)
        {
            auto parent = idx;

            for(size_t child; ; parent = child)
            {
                child = (parent + 1) * 2;

                if (child >= length)
                {
                    // Leftover left node
                    if (child == length)
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
        @disable this(this);

        this(ref Store s) @trusted
        {
            store = &s;
        }

        @property
        {
            auto length() const { return store.length; }
            bool empty() const { return length == 0; }
            auto front() { return store.front; }
        }

        void popFront()
        {
            remove(0);
        }

        size_t put(in T entry)
        {
            store.insertBack(entry);
            return siftUp(length - 1);
        }

        void remove(size_t idx)
        {
            if (empty || idx >= length) return;

            store.swapAt(idx, length - 1);
            store.removeBack();

            siftUp(siftDown(idx));
        }
}

@("heapTest")
unittest
{
    struct Store(T, size_t Length)
    {
        import std.algorithm.mutation : swapAt;

        T[Length] data;
        size_t head;

        @property auto front() { return data[0]; }
        @property auto length() const { return head; }
        int compareAt(size_t l, size_t r) { return data[l] > data[r]; }
        void swapAt(size_t l, size_t r) { data.swapAt(l, r); }
        void insertBack(T val) { data[head++] = val; }
        void removeBack() { head--; }
    }

    Store!(int, 10) arr;

    with (BinaryHeap!(typeof(arr))(arr))
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
