module unio.primitives.queue;

@safe @nogc:

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
 */
public struct RingBuffer(E, size_t Size)
{
pure nothrow:
    enum Capacity = Size + 1;

private:
    E[Capacity] data;
    size_t head = 0, tail = 0;

    size_t next(in size_t val) const
    {
        return (val + 1) % Capacity;
    }

public:
    @property
    {
        bool empty() const { return tail == head; }
        bool full() const { return next(tail) == head; }
        E front() const { return data[head]; }
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

    void put(E elem)
    {
        immutable nextTail = next(tail);

        if (nextTail != head) {
            data[tail] = elem;
            tail = nextTail;
        }
    }
}
