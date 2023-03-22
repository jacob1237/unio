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
    import std.typecons : Nullable, nullable;
    import unio.primitives.allocator : isStaticAllocator;
    import unio.primitives.array : Array;

    public alias Key = uint;

    private:
        struct Slot
        {
            Nullable!T entry;
            Key next;
        }

        size_t _length;
        Key next;
        Array!(Slot, Allocator) store;

    public:
        @disable this(this);

        this(size_t capacity)
        {
            store = typeof(store)(capacity, false);
        }

        static if (!isStaticAllocator!Allocator)
        this(Allocator allocator, size_t capacity)
        {
            store = typeof(store)(allocator, capacity, false);
        }

        @property
        {
            bool empty() const { return length == 0; }
            size_t capacity() const { return store.capacity; }
            size_t length() const { return _length; }
        }

        bool opBinaryRight(string op : "in")(Key key) const
        {
            if (key == 0) return false;

            const idx = key - 1;
            return idx < store.length && !store[idx].entry.isNull;
        }

        Key put(T val)
        {
            const key = Key(next + 1);

            if (next == store.length)
            {
                store.insertBack(Slot(nullable(val), key));
                next = key;
            }
            else
            {
                const nextFree = store[next].next;
                store[next] = Slot(nullable(val), nextFree);
                next = nextFree;
            }

            _length++;

            return key;
        }

        void take(Found)(Key key, scope Found f)
        {
            const idx = key - 1;
            if (key in this) f(store[idx].entry.get);
        }

        auto take(Found, NotFound)(Key key, scope Found f, scope NotFound nf)
        {
            const idx = key - 1;
            return key in this ? f(store[idx].entry.get) : nf();
        }

        void remove(Key key)
        {
            if (key !in this) return;

            const idx = key - 1;

            with (store[idx])
            {
                entry.nullify();
                next = this.next;
            }

            next = idx;
            _length--;
        }

        void reset()
        {
            next = 0;
            _length = 0;
            store.reset();    
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
    assert(p.store.capacity == capacity);
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

@("poolPutRemove")
unittest
{
    auto p = ArrayPool!(uint, TestAlloc)(5);

    assert(p.put(1) == 1);
    assert(p.put(2) == 2);
    assert(p.put(3) == 3);

    p.remove(2);
    p.remove(1);

    assert(p.put(4) == 1);
    assert(p.put(5) == 2);
    assert(p.put(6) == 4);

    assert(p.length == 4);
}

@("poolPutRandom")
unittest
{
    auto p = ArrayPool!(uint, TestAlloc)(5);

    assert(p.put(1) == 1);
    assert(p.put(2) == 2);
    assert(p.put(3) == 3);
    p.remove(1);           // next: 0, prev: 3
    assert(p.put(4) == 1); // next: 3, prev: 0
    p.remove(2);           // next: 1, prev: 3
    assert(p.put(5) == 2); // next: 3, prev: 1
    p.remove(1);           // next: 0, prev: 3
    p.remove(2);           // next: 1, prev: 0
    assert(p.put(6) == 2); // next: 0, prev: 1
    assert(p.put(7) == 1); // next: 3, prev: 0
}
