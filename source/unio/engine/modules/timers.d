module unio.engine.modules.timers;

private import core.time;

@safe:

public struct Timer
{
    alias expiration this;
    private alias This = typeof(this);

    private:
        size_t id;
        MonoTime exp;

    public:
        @disable this();
        this(in Duration dur) @nogc { exp = MonoTime.currTime + dur; }

        @property
        {
            bool expired() @nogc const { return exp <= MonoTime.currTime(); }
            auto expiration() @nogc const { return exp; }
        }
}

/** 
The Timers struct is a priority queue for the user-defined timers.

Since the timers data is expected to be stored in a separate storage, the queue provides
facilities for adding or removing a `Timer` by `Key`, which is assigned by the external storage.

The data is then resolved from that storage via the `Resolver` delegate, thus, allowing
the end user to define his own storage entry structure. The only requirement is that the
resolver must return a `ref` to the `Timer` struct.

This particular implementation uses a binary heap to store timer keys in the internal
dynamic array. However, when the user removes or inserts a timer, the other affected
timers are automatically updated in order to capture their specific array indexes
for future removal/cancellation (by `Timer.id`).

The Timers interface itself is designed to allow for other implementations, say, Red-Black Tree or Timer Wheel.
For instance, the Red-Black Tree won't require any internal containers, only the `left`/`right`/`parent`
pointers under the Timer struct.
*/
public struct Timers(Key, Allocator)
{
    import std.typecons : NullableRef;
    import unio.primitives.allocator : isStaticAllocator;
    import unio.primitives.queue : BinaryHeap;

    private alias MinHeap = BinaryHeap!(Store);
    public alias Resolver = NullableRef!Timer delegate (Key) nothrow @nogc;

    private struct Store
    {
        import unio.primitives.array : Array;

        Array!(Key, Allocator) data;
        Resolver resolve;

        alias data this;

        int compareAt(size_t a, size_t b) @nogc
        {
            auto left = resolve(data[a]);
            auto right = resolve(data[b]);

            return !left.isNull && !right.isNull ? left > right : 0;
        }

        /** 
        We hook BinaryHeap's swapAt method to keep task data and timer IDs
        in sync to be able to remove (cancel) the arbitrary timers later
        */
        void swapAt(size_t a, size_t b) @nogc
        {
            const leftId = data[a];
            const rightId = data[b];

            data[a] = rightId;
            data[b] = leftId;

            auto left = resolve(leftId);
            auto right = resolve(rightId);

            if (!left.isNull && !right.isNull)
            {
                const tmp = left.id;
                left.id = right.id;
                right.id = tmp;
            }
        }
    }

    private Store store;

    public:
        @disable this(this);

        this(size_t capacity, Resolver r) @nogc
        {
            store.data = typeof(store.data)(capacity);
            store.resolve = r;
        }

        static if (!isStaticAllocator!Allocator)
        this(auto ref Allocator alloc, size_t capacity, Resolver r) @nogc
        {
            store.data = typeof(store.data)(alloc, capacity);
            store.resolve = r;
        }

        @property
        {
            bool empty() const @nogc { return store.empty; }
            auto front() const @nogc { return store.empty ? Key.init : store.data[0]; }
            size_t length() const @nogc { return store.length; }
        }

        /**
        Enqueue a new timer by the given key from the corresponding data store

        Params:
          key = Timer unique identifier assigned by the user-defined store
        */
        void put(Key key) @nogc
        {
            auto timer = store.resolve(key);

            if (!timer.isNull)
            {
                // Assign the timer ID beforehand, because `store.swapAt()`
                // will have troubles swapping uninitialized Timer structs
                timer.id = store.data.length;
                MinHeap.insert(store, key);
            }
        }

        void remove(in Timer timer) @nogc
        {
            MinHeap.remove(store, timer.id);
        }
}

@("timersPutRemove")
unittest
{
    import std.experimental.allocator.mallocator : Mallocator;
    import std.typecons : NullableRef;

    alias Key = size_t;

    struct Entry
    {
        size_t id;
        Timer timer;
    }

    Entry[3] data = [
        Entry(1, Timer(10.msecs)),
        Entry(2, Timer(3.msecs)),
        Entry(3, Timer(5.msecs)),
    ];

    auto resolve = (Key k) => NullableRef!Timer(data.length > k - 1 ? &data[k - 1].timer : null);
    auto t = Timers!(Key, Mallocator)(data.length, resolve);

    t.put(1);
    t.put(2);
    t.put(3);
    assert(t.length == 3);
    assert(t.front == 2);

    // Check whether the timer structs are correctly updated after inserting into the queue
    assert(resolve(1).id == 1);
    assert(resolve(2).id == 0); 
    assert(resolve(3).id == 2);

    t.remove(resolve(2));
    assert(t.length == 2);
    assert(t.front == 3);

    t.remove(resolve(3));
    assert(t.length == 1);
    assert(t.front == 1);
}

public struct TimerFd
{
    import core.sys.linux.timerfd;

    private:
        int _fd;

    public:
        @disable this();
        @disable this(this);

        ~this() @nogc
        {
            import core.sys.posix.unistd : close;
            close(_fd);
        }

        static typeof(this) make() @trusted @nogc
        {
            typeof(this) inst = void;
            inst._fd = timerfd_create(CLOCK_MONOTONIC, TFD_NONBLOCK);

            return inst;
        }

        @property int fd() const @nogc { return _fd; }

        void arm(in MonoTime exp) @trusted @nogc
        {
            itimerspec spec = {
                it_value: cast(timespec) exp.ticks.ticksToNSecs.nsecs.split!("seconds", "nsecs")
            };

            timerfd_settime(fd, TFD_TIMER_ABSTIME, &spec, null);
        }

        void disarm() @trusted @nogc
        {
            const itimerspec zero;
            timerfd_settime(fd, 0, &zero, null);
        }
}
