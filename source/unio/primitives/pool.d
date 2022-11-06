module unio.primitives.pool;

@safe @nogc:

public import std.typecons : NullableRef, nullableRef;

/** 
 * The struct represents a key (or ID) that could be used to track specific operations
 */
struct Key
{
    size_t val;
    alias val this;

    this(size_t v)
    {
        val = v;
    }

    bool opCast(T : bool)() const
    {
        return val != Key(0);
    }
}

public interface Pool(T)
{
    @property
    {
        size_t capacity() const;
        size_t length() const;
    }

    void resize(in size_t capacity);

    NullableRef!T get(Key);
    bool has(Key) const;

    Key add(T);
    bool replace(Key, T);
    void remove(Key);

    void clear();
}

/**
 * The object pool implementation using freelist.
 * This object pool completely owns its internal references, so nothing can escape it.
 */
public class FreeList(T, Allocator) : Pool!T
{
    import std.experimental.allocator :
        makeArray,
        shrinkArray,
        expandArray,
        dispose;

    enum MIN_CAPACITY = 1;

protected:
    struct Slot
    {
        T entry;
        size_t next;
    }

    size_t _capacity, _length;
    size_t chunkSize;
    size_t next = 0;
    public Slot[] data;

    static auto initializer(size_t begin, size_t end) pure
    {
        import std.range : iota, chain, only;
        import std.algorithm.iteration : map;

        return iota(begin, end).map!((index) => Slot(T.init, index + 1));
    }

public:
    this(in size_t capacity = MIN_CAPACITY) @trusted
    {
        _capacity = capacity < MIN_CAPACITY ? MIN_CAPACITY : capacity;
        chunkSize = _capacity;
        data = makeArray!(Slot)(Allocator.instance, initializer(0, _capacity));
    }

    ~this() @trusted
    {
        dispose(Allocator.instance, data);
    }

    @property
    {
        bool empty() const
        {
            return length() == 0;
        }

        size_t capacity() const
        {
            return _capacity;
        }

        size_t length() const
        {
            return _length;
        }
    }

    void resize(in size_t newCapacity) @trusted
    {
        const delta = newCapacity - _capacity;

        if (delta == 0) {
            return;
        }
        else if (delta > 0) {
            expandArray(Allocator.instance, data, initializer(_capacity, newCapacity));
        }
        else if (length <= newCapacity) {
            shrinkArray(Allocator.instance, data, -delta);
        }

        _capacity += delta;
    }

    Key add(T val)
    {
        if (length == _capacity - 1) {
            resize(_capacity + chunkSize);
        }

        immutable key = Key(next + 1);
        immutable nextFree = data[next].next;

        data[next] = Slot(val, nextFree);

        next = nextFree;
        _length++;

        return key;
    }

    NullableRef!(T) get(Key k) @trusted
    {
        return nullableRef(
            has(k) ? &data[k - 1].entry : null
        );
    }

    bool has(Key k) const
    {
        return k && k <= _capacity && data[k - 1].entry != T.init;
    }

    bool replace(Key k, T val)
    {
        if (!has(k)) {
            return false;
        }

        data[k - 1].entry = val;
        return true;
    }

    void remove(Key k)
    {
        if (has(k)) {
            _length--;
            data[k - 1] = Slot(T.init, next);

            next = k - 1;
        }
    }

    void clear()
    {
        next = 0;
        _length = 0;

        auto padding = initializer(0, _capacity);

        for (size_t i = 0; !padding.empty; padding.popFront, ++i) {
            data[i] = padding.front;
        }
    }
}
