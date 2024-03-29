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

TODO: Use the custom FreeList allocator on top of the Allocator internally to prevent performance issues
TODO: Write proper unit tests for each Table method and a couple of functional tests
TODO: Allow the RowLength to be defined at runtime (dynamically)
*/
public struct Table(T, size_t RowLength, Allocator)
{
    import core.stdc.string : memset;
    import unio.primitives.allocator : makeArray, resizeArray, isStaticAllocator;

    public enum minTableLen = 1;

    private:
        struct Position
        {
            size_t row;
            size_t col;

            @disable this();

            this(size_t idx)
            {
                row = idx / RowLength;
                col = idx % RowLength;
            }
        }

        struct Row
        {
            import core.bitop : bt, bts, btr;

            size_t counter; // A number of non-empty elements
            ubyte[RowLength / 8] index; // A bitset for null checks
            T[RowLength] data;

            @property bool empty() const pure nothrow
            {
                return counter == 0;
            }

            T* opIndex()(size_t col) pure nothrow
            {
                return &data[col];
            }

            T* opIndexAssign(T)(scope ref T val, size_t col) pure nothrow @trusted
            {
                bts(cast(ulong*) index, col);
                data[col] = val;
                counter++;

                return &data[col];
            }

            void remove(const size_t col) pure nothrow @trusted
            {
                btr(cast(ulong*) index, col);
                counter--;
            }

            bool opBinaryRight(string op : "in")(size_t col) pure nothrow @trusted
            {
                return cast(bool) bt(cast(ulong*) index, col);
            }
        }

        Row*[] table;
        size_t tableStartLen;

        static if (isStaticAllocator!Allocator) alias alloc = Allocator.instance;
        else Allocator alloc;

        T* lookup(in Position pos) pure nothrow
        {
            with (pos)
            return row < table.length && table[row] !is null && col in *table[row]
                ? (*table[row])[col]
                : null;
        }

        /**
        Insert element at the specific position.

        When the requested row is beyond the growth factor values, the table index
        is extended just up to that row, otherwise, the length grows twice its size.

        If the row doesn't exist, it will be allocated.
        */
        T* insert(in Position pos, T val)
        {
            import std.algorithm.comparison : max;

            with (pos)
            {
                if (row >= table.length) alloc.resizeArray(table, max(table.length << 1, row + 1));
                if (table[row] is null) makeRow(row);

                return (*table[row])[col] = val;
            }
        }

        /**
        Allocate new row for a particular index
        */
        void makeRow(const size_t row) @trusted
        {
            auto addr = alloc.allocate(Row.sizeof);
            if (addr is null) assert(false, "Can't allocate Table row");

            auto rowPtr = cast(Row*) addr.ptr;
            resetRow(*rowPtr);

            table[row] = rowPtr;
        }

        /**
        Deallocate a particular range of rows and nullify the pointers
        */
        void removeRow(Row*[] slice) @trusted
        {
            foreach (ref row; slice)
            {
                if (row !is null) {
                    alloc.deallocate(row[0 .. Row.sizeof]);
                    row = null;
                }
            }
        }

        /**
        Reset/re-initialize a bitset and a counter for the row
        */
        void resetRow(ref Row row) @trusted
        {
            memset(&row.index, 0, Row.index.sizeof);
            row.counter = 0;
        }

        void initialize(size_t startLen)
        {
            tableStartLen = startLen < minTableLen ? minTableLen : startLen;
            table = alloc.makeArray!(Row*)(tableStartLen);
        }

    public:
        @disable this(this);

        /*
        Params:
          startLen = Initial row index size
        */
        this(size_t startLen)
        {
            initialize(startLen);
        }

        static if (!isStaticAllocator!Allocator)
        this(Allocator allocator, size_t startLen)
        {
            alloc = allocator;
            initialize(startLen);
        }

        ~this() @trusted
        {
            if (table.length)
            {
                removeRow(table[]);
                alloc.deallocate(table);
            }
        }

        ref T opIndexAssign(T)(T val, size_t idx) return
        {
            return *insert(Position(idx), val);
        }

        bool opBinaryRight(string op : "in")(size_t idx) pure nothrow
        {
            return cast(bool) lookup(Position(idx));
        }

        /** 
        Takes value from the array and runs a delegate on it if exists.
        If the value is not present, executes the notFound() callback
        */
        void take(Fn)(const size_t idx, scope Fn found)
        {
            auto entry = lookup(Position(idx));
            if (entry !is null) found(*entry);
        }

        /** ditto */
        auto take(Found, NotFound)(const size_t idx, scope Found found, scope NotFound notFound)
        {
            auto entry = lookup(Position(idx));
            return entry !is null ? found(*entry) : notFound();
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

            return entry !is null ? *entry : *insert(pos, newVal);
        }

        /**
        Removes an element by the index
        When all indexes of the same row are empty, the row is freed automatically
        */
        void remove(const size_t idx)
        {
            auto pos = Position(idx);
            if (!lookup(pos)) return;

            auto row = table[pos.row];
            row.remove(pos.col);

            if (row.empty) removeRow(table[pos.row .. pos.row + 1]);
        }

        /** 
        Removes all rows except the first one and shrinks the table index to its start length
        */
        void reset()
        {
            if (!table.length) return;

            removeRow(table[1 .. $]);
            resetRow(*table[0]);

            alloc.resizeArray(table, tableStartLen);
            table[0 .. $] = null;
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
unittest
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

    // Check in accordance with the growth factor (length x 2)
    assert(t.table.length == 6);

    t.remove(10);
    assert(t.table[3] is null);
    assert(Alloc.instance.numDeallocate == 1);
}
