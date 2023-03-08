module unio.primitives.pool;

@safe @nogc:

public enum isPool(T) = 
    is(T.length == size_t) &&
    is(T.capacity == size_t) &&
    is(T.put == ArrayPool.Key) &&
    is(T.remove == void) &&
    is(T.reset == void);

/** 
This pool implements a free-list style object pool on top of the dynamic array.
The array iself grows via `realloc()` with the growth factor 2.

All objects/values are only accesible via `take()`, which prevents pointers escape.
Also, the values are nullable by default, so the end user doesn't have to care about it.

TODO: Add support for auto-shrink after certain threshold
TODO: Define invalid Key value for additional checks
TODO: Think how to get rid of `realloc` (unrolled linked list with memory addresing?)
*/
public struct ArrayPool(T, Allocator)
{
    import core.checkedint : mulu;
    import core.stdc.string : memset;
    import std.typecons : Nullable, nullable;
    import unio.primitives.allocator : isStaticAllocator;

    public alias Key = uint;

    private:
        enum minCapacity = 1;

        struct Slot
        {
            Nullable!T entry;
            Key next;
        }

        static if (isStaticAllocator!Allocator) alias alloc = Allocator.instance;
        else Allocator alloc;

        size_t _length;
        size_t initialCapacity;
        Key next;
        Slot[] data;

        /*
        Struct initializer that is used as a part of struct constructors
        */
        void initialize(size_t capacity) @trusted
        {
            initialCapacity = capacity < minCapacity ? minCapacity : capacity;

            bool overflow;
            const size = mulu(initialCapacity, Slot.sizeof, overflow);
            if (overflow) assert(false, "ArrayPool length overflow");

            const mem = alloc.allocate(size);
            if (mem is null) assert(false, "Can't allocate ArrayPool");

            data = cast(typeof(data)) mem;

            nullify(data);
        }

        /*
        Zero-initialize a particular slice of the internal "data" array
        */
        void nullify(Slot[] slice) @trusted
        {
            auto mem = cast(void[]) slice;
            memset(mem.ptr, 1, mem.length);
        }

        /**
        Resize the internal array to the given length (not size!)
        */
        void resize(size_t len) @trusted
        {
            if (len == data.length) return;

            bool overflow;
            const size = mulu(len, Slot.sizeof, overflow);
            if (overflow) assert(false, "ArrayPool length overflow");

            auto mem = cast(void[]) data;
            if (!alloc.reallocate(mem, size)) assert(false, "Can't resize ArrayPool");

            data = cast(typeof(data)) mem;
        }

        /** 
        Grow the internal with growth factor x2

        TODO: Grow in accordance with the system page size to speed-up reallocs
        */
        void grow() @trusted
        {
            const oldLen = data.length;
            resize(data.length << 1);
            nullify(data[oldLen .. $]);
        }

    public:
        @disable this(this);

        this(size_t capacity)
        {
            initialize(capacity);
        }

        static if (!isStaticAllocator!Allocator)
        this(Allocator allocator, size_t capacity)
        {
            alloc = allocator;
            initialize(capacity);
        }

        ~this() @trusted
        {
            alloc.deallocate(cast(void[]) data);
            data = null;
        }

        @property
        {
            bool empty() const { return length == 0; }
            size_t capacity() const { return data.length; }
            size_t length() const { return _length; }
        }

        bool opBinaryRight(string op : "in")(Key key) const
        {
            if (key == 0) return false;

            const idx = key - 1;
            return capacity > idx && !data[idx].entry.isNull;
        }

        Key put(T val)
        {
            if (length >= capacity) grow();

            const key = Key(next + 1);
            const nextFree = next == length ? key : data[next].next;

            data[next] = Slot(nullable(val), nextFree);
            next = nextFree;
            _length++;

            return key;
        }

        void take(Found)(Key key, scope Found f)
        {
            const idx = key - 1;
            if (key in this) f(data[idx].entry.get);
        }

        auto take(Found, NotFound)(Key key, scope Found f, scope NotFound nf)
        {
            const idx = key - 1;
            return key in this ? f(data[idx].entry.get) : nf();
        }

        void remove(Key key)
        {
            if (key !in this) return;

            const idx = key - 1;

            data[idx].entry.nullify();
            next = idx;
            _length--;
        }

        void reset()
        {
            next = 0;
            _length = 0;

            resize(initialCapacity);
            nullify(data);
        }
}

version(unittest)
{
    import std.experimental.allocator.mallocator : TestAlloc = Mallocator;

    struct User
    {
        size_t id;
        string name;
    }
}

@("poolOpIn")
unittest
{
    auto p = ArrayPool!(User, TestAlloc)(2);
    assert(0 !in p);
    assert(1 !in p);
    assert(2 !in p);
    assert(3 !in p);

    const key = p.put(User(10, "Test User"));
    assert(key in p);
    assert(2 !in p);

    p.remove(key);
    assert(key !in p);
}

@("poolPut")
unittest
{
    static immutable capacity = 3;
    static immutable users = [User(10, "John"), User(20, "Bob"), User(30, "Simon")];

    auto p = ArrayPool!(User, TestAlloc)(capacity);
    assert(p.data.length == capacity);
    assert(p.length == 0);

    const k1 = p.put(users[0]);
    assert(p.length == 1);
    assert(k1 == 1);
    assert(users[0] == p.take(k1, (ref User u) => u, () => User.init));

    const k2 = p.put(users[1]);
    assert(p.length == 2);
    assert(k2 == 2);
    assert(users[1] == p.take(k2, (ref User u) => u, () => User.init));

    const k3 = p.put(users[2]);
    assert(p.length == 3);
    assert(k3 == 3);
    assert(users[2] == p.take(k3, (ref User u) => u, () => User.init));
}

@("poolCapacity")
unittest
{
    static immutable capacity = 2;

    auto p = ArrayPool!(size_t, TestAlloc)(capacity);
    assert(p.capacity == capacity);

    foreach (i; 0 .. 2) p.put((i + 1) * 10);
    assert(p.capacity == capacity);

    p.put(40);
    assert(p.capacity == capacity * 2);
}

@("poolRemove")
unittest
{
    auto p = ArrayPool!(size_t, TestAlloc)(2);
    const k1 = p.put(10);
    const k2 = p.put(20);

    p.remove(k1);
    assert(k1 !in p);
    assert(k2 in p);
    assert(p.length == 1);

    p.remove(k2);
    assert(k1 !in p);
    assert(k2 !in p);
    assert(p.length == 0);
}

@("poolReset")
unittest
{
    static immutable capacity = 2;

    auto p = ArrayPool!(User, TestAlloc)(capacity);
    auto usr = User(10, "Test Reset");

    foreach (i; 0 .. capacity * 4) p.put(usr);
    assert(p.length == capacity * 4);
    assert(p.capacity == capacity * 4);

    p.reset();
    assert(p.length == 0);
    assert(p.capacity == capacity);
    assert(1 !in p);
    assert(2 !in p);
}
