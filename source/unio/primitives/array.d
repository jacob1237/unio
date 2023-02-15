module unio.primitives.array;

@safe:

/** 
A primitive dynamic array container implementation.

Facts:
  - Serves as a base for other containers
  - Doesn't grow by itself, `resize()` must be called
  - Doesn't support insertion, only direct indexing
*/
struct Array(T, Allocator)
{
    import core.checkedint : mulu;
    import core.stdc.string : memset;

    private:
        T[] data;

        void initialize(size_t len) @trusted
        {
            bool overflow;
            auto size = mulu(len, T.sizeof, overflow);
            if (overflow) assert(false, "Array length overflow");

            auto mem = alloc.allocate(size);
            if (mem is null) assert(false, "Can't allocate Array");

            data = cast(typeof(data)) mem;
            memset(mem.ptr, 0, mem.length);
        }

    public:
        enum isStaticAllocator = __traits(hasMember, Allocator, "instance");

        static if (isStaticAllocator) alias alloc = Allocator.instance;
        else Allocator alloc;

        @disable this(this);

        this(size_t len)
        {
            initialize(len);
        }

        static if (!isStaticAllocator)    
        this(Allocator allocator, size_t len)
        {
            alloc = allocator;
            initialize(len);
        }

        ~this() @trusted
        {
            alloc.deallocate(cast(void[]) data);
        }

        @property size_t length() const { return data.length; }

        ref opIndex(size_t idx) { return data[idx]; }
        auto opSlice() { return data[0 .. $]; }
        auto opSlice(size_t start, size_t end) { return data[start .. end]; }
        auto opSliceAssign(T)(T val) { return data[0 .. $] = val; }
        auto opSliceAssign(T)(T value, size_t start, size_t end) { return data[start .. end] = value; }

        alias opDollar = length;

        /** 
        Resizes the array to specific length
        */
        void resize(size_t newLen) @trusted
        {
            import std.stdio;

            const oldLen = data.length;

            if (newLen == oldLen || newLen == 0) return;

            bool overflow;
            const newSize = mulu(newLen, T.sizeof, overflow);
            if (overflow) assert(false, "Array length overflow");

            auto mem = cast(void[]) data;
            if (!alloc.reallocate(mem, newSize)) assert(false, "Can't resize Array");

            data = cast(typeof(data)) mem;

            // Zero-initialize the array if resize is a grow operation
            if (newLen > oldLen)
            {
                mem = cast(void[]) data[oldLen .. $];
                memset(mem.ptr, 0, mem.length);
            }
        }
}

version(unittest)
import std.experimental.allocator.mallocator;

@("arrayCreate")
unittest
{
    auto a = Array!(int, Mallocator)(3);
    assert(a.length == 3);
    assert(a[] == [0, 0, 0]);
}

@("arrayResize")
unittest
{
    auto a = Array!(int, Mallocator)(3);
    a.resize(6);
    assert(a.length == 6);
    assert(a[] == [0, 0, 0, 0, 0, 0]);

    a.resize(2);
    assert(a.length == 2);
    assert(a[] == [0, 0]);
}

@("arrayOpSlice")
unittest
{
    auto a = Array!(int, Mallocator)(3);
    a[] = [1, 2, 3];
}

@("arrayOpSliceAssign")
unittest
{
    auto a = Array!(int, Mallocator)(3);
    a[1 .. $] = [2, -4];
    assert(a[] == [0, 2, -4]);

    a[] = [3, 4, 5];
    assert(a[] == [3, 4, 5]);
}

@("arrayOpIndex")
unittest
{
    auto a = Array!(int, Mallocator)(3);
    a[0 .. $] = [1, 2, 3];
    assert(a[0] == 1);
    assert(a[1] == 2);
    assert(a[2] == 3);
}

@("arrayOpIndexAssign")
unittest
{
    auto a = Array!(int, Mallocator)(3);
    a[1] = 444;
    assert(a[1] == 444);
    assert(a[] == [0, 444, 0]);
}
