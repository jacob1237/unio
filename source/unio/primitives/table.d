module unio.primitives.table;

@safe @nogc:

/** 
The Table implements a similar interface to associative array, but instead of using
a hash function, it maps sequential numeric keys into values pretty much like a simple array does.

However, there are key differences:

1. The data is chunked in rows, so when the array grows, it allocates the next chunk for new data
2. It's possible to insert an index far away from the beginning and it will not allocate
   all intermediary chunks (rows), but only the chunk holding the particular key/index
3. The chunk is freed automatically when becomes empty
4. Implements both hashmap and Option-like interface, allowing to work with empty values

TODO: Introduce an appropriate growth strategy for the index array (growth factor?)
TODO: Automatically shrink the index array when it becomes partially empty
TODO: Use the custom FreeList allocator on top of the Allocator internally to prevent performance issues
TODO: Write proper unit tests for each Table method and a couple of functional tests
TODO: Allow the RowLength to be defined at runtime (dynamically)
TODO: Add user-defined empty/null checks to the element types instead of comparing with T.init
*/
public struct Table(T, size_t RowLength, Allocator)
{
    import std.experimental.allocator : make, dispose, makeArray, expandArray;

    private:
        struct Position
        {
            size_t row;
            size_t col;

            this(size_t idx)
            {
                row = idx / RowLength;
                col = idx % RowLength;
            }
        }

        struct Row
        {
            T[RowLength] data;
            size_t counter; // A number of non-empty elements 
        }

        enum isStaticAllocator = __traits(hasMember, Allocator, "instance");

        static if (isStaticAllocator) alias alloc = Allocator.instance;
        else Allocator alloc;

        Row*[] table;

        T* lookup(in Position pos) pure nothrow
        {
            return pos.row < table.length && table[pos.row] !is null
                ? &table[pos.row].data[pos.col]
                : null;
        }

        T* insert(in Position pos, T val) @trusted
        {
            const delta = pos.row + 1 - table.length;

            if (pos.row >= table.length && !alloc.expandArray(table, delta)) {
                assert(false, "Can't expand Table index");
            }

            if (table[pos.row] is null)
            {
                auto ptr = alloc.make!Row;

                if (ptr is null) {
                    assert(false, "Can't allocate Table row");
                }

                table[pos.row] = ptr;
            }

            with (table[pos.row])
            {
                data[pos.col] = val;
                counter++;

                return &data[pos.col];
            }
        }

        void initialize(size_t startLen) @trusted
        {
            if (table is null) {
                table = alloc.makeArray!(Row*)(startLen ? startLen : RowLength);
            }
        }

        static bool empty(in T* entry) pure nothrow
        {
            return entry is null || *entry == T.init;
        }

    public:
        this(size_t startLen)
        {
            initialize(startLen);
        }

        static if (!isStaticAllocator)
        this(Allocator allocator, size_t startLen)
        {
            alloc = allocator;
            initialize(startLen);
        }

        ~this() @trusted
        {
            if (table.length)
            {
                foreach (row; table) if (row !is null) alloc.dispose(row);
                alloc.dispose(table);
            }
        }

        ref T opIndexAssign(T)(T val, size_t idx) return
        {
            return *insert(Position(idx), val);
        }

        bool opBinaryRight(string op : "in")(size_t idx) pure nothrow
        {
            return !empty(lookup(Position(idx)));
        }

        /** 
        Takes value from the array and runs a delegate on it if exists.
        If the value is not present, executes the notFound() callback
        */
        void take(Fn)(const size_t idx, scope Fn found)
        {
            auto entry = lookup(Position(idx));
            if (!empty(entry)) found(*entry);
        }

        /** ditto */
        auto take(Found, NotFound)(const size_t idx, scope Found found, scope NotFound notFound)
        {
            auto entry = lookup(Position(idx));
            return !empty(entry) ? found(*entry) : notFound();
        }

        /** 
        Similar to AAs `require()`, the function returns a reference to the table entry
        or creates a new one from the given initial value before returning
        
        Params:
          idx = Index of the entry
          newVal = User-defined default value
        Returns:
          Reference to the value within the table
        */
        ref T require(const size_t idx, lazy T newVal = T.init) return
        {
            const pos = Position(idx);
            auto entry = lookup(pos);

            return !empty(entry) ? *entry : *insert(pos, newVal);
        }

        void remove(const size_t idx) @trusted
        {
            auto pos = Position(idx);

            if (empty(lookup(pos))) return;

            with (table[pos.row])
            {
                data[pos.col] = T.init;
                counter--;

                // Free the row memory block if there are no more elements
                if (counter == 0) {
                    alloc.dispose(table[pos.row]);
                    table[pos.row] = null;
                }
            }
        }
}

@("tableOpBinaryIn")
unittest
{
    import std.experimental.allocator.mallocator : Mallocator;

    struct Entry
    {
        size_t id;
        string name;
    }

    auto t = Table!(Entry, 8, Mallocator)(8);

    // Test not found
    assert(2 !in t);
    assert(t.take(2, (ref Entry _) => false, () => true));

    // Test assignment for the first row
    static const testEntry = Entry(10, "Test");
    t[2] = testEntry;

    assert(1 !in t);
    assert(2 in t);
    assert(3 !in t);
    assert(testEntry == t.take(2, (ref Entry e) => e, () => Entry.init));

    // Test assignment for a distant row
    static const testEntry2 = Entry(25, "Test");
    t[127] = Entry(25, "Test");

    assert(126 !in t);
    assert(127 in t);
    assert(128 !in t);
    assert(testEntry2 == t.take(127, (ref Entry e) => e, () => Entry.init));
}

@("tableGrowShrink")
@trusted unittest
{
    import std.experimental.allocator.mallocator : Mallocator;
    import std.experimental.allocator.building_blocks.stats_collector : StatsCollector, Options;

    struct Alloc
    {
        static StatsCollector!Mallocator instance;
        alias instance this;
    }

    alias Entry = long;

    auto t = Table!(Entry, 3, Alloc)(3);
    assert(t.table.length == 3);

    t[10] = Entry(777);
    assert(t.table.length == 4);

    t.remove(10);
    assert(t.table[3] is null);
    assert(Alloc.instance.numDeallocate == 1);
}
