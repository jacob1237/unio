module unio.primitives.array;

@safe @nogc:

/** 
A very primitive array implementation which serves as a building block for other
data structures like BinaryHeap or Table.

Allows inserting elements to the back of the array and growth automatically (x2) when
there is not enough space for new ones.

TODO: Add support for auto-shrink after certain threshold (resizeArray)
*/
struct Array(T, Allocator)
{
    import unio.primitives.allocator : makeArray, resizeArray, isStaticAllocator;

    enum defaultCapacity = 8;

    private:
        size_t head;
        T[] data;
        size_t initialCapacity;

        static if (isStaticAllocator!Allocator) alias alloc = Allocator.instance;
        else Allocator alloc;

        void initialize(size_t capacity, bool init = true) @trusted
        {
            initialCapacity = capacity ? capacity : defaultCapacity;
            data = alloc.makeArray!(T)(initialCapacity, init);
        }

    public:
        @disable this(this);

        this(size_t capacity, bool init = true)
        {
            initialize(capacity, init);
        }

        static if (!isStaticAllocator!Allocator)
        this(Allocator allocator, size_t capacity, bool init = true)
        {
            alloc = allocator;
            initialize(capacity, init);
        }

        ~this() @trusted
        {
            if (data !is null)
            {
                alloc.deallocate(cast(void[]) data);
                data = null;
            }
        }

        @property
        {
            bool empty() const { return !head; }
            size_t capacity() const { return data.length; }
            size_t length() const { return head; }
            ref inout(T) front() inout { return data[0]; }
        }

        ref inout(T) opIndex(size_t idx) inout
        {
            return data[idx];
        }

        alias opDollar = length;

        auto opSlice(size_t start, size_t end)
        {
            return data[start .. end];
        }

        void insertBack(T val)
        {
            if (head >= capacity) alloc.resizeArray(data, capacity << 1);
            data[head++] = val;
        }

        void removeBack()
        {
            if (head) head--;
        }

        void reset()
        {
            head = 0;
            alloc.resizeArray(data, initialCapacity);
        }
}

version(unittest)
import std.experimental.allocator.mallocator : Mallocator;

@("arrayConstructor")
unittest
{
    static immutable capacity = 3;

    auto arr = Array!(int, Mallocator)(capacity);
    assert(arr.capacity == capacity);
    assert(arr.length == 0);
}

@("arrayInsertBack")
unittest
{
    auto arr = Array!(int, Mallocator)(3);

    arr.insertBack(100);
    assert(arr.capacity == 3);
    assert(arr.length == 1);
    assert(arr[0] == 100);

    // Test if it grows
    static immutable values = [1, 2, 3];

    foreach (val; values) arr.insertBack(val);
    assert(arr.capacity == 6);
    assert(arr.length == 4);
    assert(arr[0 .. $] == [100, 1, 2, 3]);
}

@("arrayRemoveBack")
unittest
{
    auto arr = Array!(int, Mallocator)(3);

    static immutable values = [2, 3, 5];

    foreach (val; values) arr.insertBack(val);
    arr.removeBack();
    assert(arr.capacity == 3);
    assert(arr.length == 2);

    arr.removeBack();
    arr.removeBack();
    assert(arr.capacity == 3);
    assert(arr.length == 0);

    // Check if it silently skips empty removals
    arr.removeBack();
    assert(arr.length == 0);
}
