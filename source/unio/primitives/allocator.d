module unio.primitives.allocator;

private import core.checkedint : mulu;
private import core.stdc.string : memset;

enum isStaticAllocator(Allocator) = __traits(hasMember, Allocator, "instance");

/**
Allocates array of the specific length and zero-initializes it if required.
*/
T[] makeArray
    (T, Allocator)
    (auto ref Allocator alloc, size_t length, bool init = true)
@trusted
{
    bool overflow;
    auto size = mulu(length, T.sizeof, overflow);
    if (overflow) assert(false, "Array length overflow");

    auto mem = alloc.allocate(size);
    if (mem is null) assert(false, "Can't allocate array");

    if (init) memset(mem.ptr, 0, mem.length);

    return cast(T[]) mem;
}

/** 
Resize array to a specific length.
If resize is a grow operation, the newly allocated block will be zeroed. 
*/
bool resizeArray
    (T, Allocator)
    (auto ref Allocator alloc, ref T[] arr, size_t newLen)
@trusted
{
    const oldLen = arr.length;

    if (newLen == oldLen || newLen == 0) return false;

    bool overflow;
    const newSize = mulu(newLen, T.sizeof, overflow);
    if (overflow) assert(false, "Array length overflow");

    auto mem = cast(void[]) arr;
    if (!alloc.reallocate(mem, newSize)) assert(false, "Can't resize Array");

    arr = cast(T[]) mem;

    // Zero-initialize the array if resize is a grow operation
    if (newLen > oldLen)
    {
        mem = cast(void[]) arr[oldLen .. $];
        memset(mem.ptr, 0, mem.length);
    }

    return true;
}
