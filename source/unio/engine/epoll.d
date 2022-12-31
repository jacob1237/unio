module unio.engine.epoll;

@safe:// @nogc:

public import unio.engine;

private:
    import core.stdc.errno;
    import core.sys.posix.netinet.in_;
    import core.sys.posix.unistd;
    import unio.primitives;

    enum SOCK_NONBLOCK = 0x800;
    extern (C) int accept4(int, sockaddr*, socklen_t*, int);

    /** 
     * Operation type enum.
     * All read tasks have lowest bits set in opposed to write-related.
     */
    enum OpType : ubyte {
        // Read tasks
        Receive = 0x01,
        Read = 0x02,
        Accept = 0x03,

        // Write tasks
        Connect = 0x10,
        Write = 0x20,
        Send = 0x30,
    }

    /** 
     * The pipeline represents a chain of tasks to be executed
     */
    struct Pipeline
    {
        enum State : ubyte { ready, notReady, error, hup }

        State state;
        Key head; // First task in the queue
        Key tail; // Last task in the queue

        @property
        {
            bool ready() { return state != State.notReady; }
            bool error() { return state == State.error; }
        }
    }

    /** 
     * File descriptor state
     */
    struct FDInfo
    {
        int fd;
        Pipeline read;
        Pipeline write;

        ref Pipeline pipeline(ref Task t) return { return t.isWrite ? write : read; }
    }

    /** 
     * Internal EPoll engine task item.
     *
     * Contains both operation data and arguments, as well as
     * pointers to the next socket operation (for easy search).
     */
    struct Task
    {
        struct Data {
            mixin Operation!int;
        }

        OpType type;
        Status.Type status;
        Data data;
        size_t next;
        long result;

        // Params
        union {
            void[] buf;
            sockaddr addr;
            sockaddr* newAddr;
        }

        int flags;
        @property bool isWrite() const { return cast(bool) (type & 0xF0); }
    }

    /** 
     * Convert operation data to a Task entry
     */
    Task toTask(Op)(Op op) @trusted
    {
        auto entry = Task(
            mixin("OpType." ~ Op.stringof),
            Status.Type.Pending,
            Task.Data(cast(size_t) op.fd, op.token)
        );

        static if (__traits(hasMember, Op, "buf")) entry.buf = op.buf;
        static if (__traits(hasMember, Op, "addr")) entry.addr = op.addr;
        static if (__traits(hasMember, Op, "newAddr")) entry.newAddr = cast(sockaddr*) op.newAddr;
        static if (__traits(hasMember, Op, "flags")) entry.flags = op.flags;

        return entry;
    }

    Result.Type toResultType(Status.Type type)
    {
        return type == Status.Type.Success ? Result.Type.Success : Result.Type.Error;
    }

    // TODO: Handle getsockopt() error as well as the fd type
    int lastSocketError(int fd) @trusted
    {
        int err;
        socklen_t len = err.sizeof;
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);

        return err;
    }

public:
    /** 
     * Epoll IO engine implementation
     *
     * TODO: Add `Cancel` support for all operations on a specific file descriptor
     * TODO: Add support for the `Wait` operation
     * TODO: Add suport for the `Timeout` operation
     * TODO: Distinguish different types of file descriptors
     * TODO: Add operation chaining
     * TODO: Add support for async file operations (via AIO/io_submit or a dedicated thread pool)
     * TODO: Handle connection errors both for read/write operations
     * TODO: Rename read/write task types to input/output
     * TODO: Make task containers more flexible and efficient
     * TODO: Edge case: epoll may notify readiness for send(), but EAGAIN will be returned: https://habr.com/ru/post/416669/#comment_18865881
     * TODO: Handle SIGPIPE correctly when using `write()`: https://stackoverflow.com/a/18963142/7695184
     * TODO: Handle setsockopt() `SO_RCVTIMEO` and `SO_SNDTIMEO` (check how do they react)
     * TODO: Call epoll_ctl(EPOLL_CTL_DEL) when there are no tasks for a file descriptor for a long time
     * TODO: Handle vectorized I/O (iovec)
     */
    class EpollEngine : IOEngine
    {
        import core.sys.linux.epoll;
        import core.sys.posix.arpa.inet;
        import core.sys.posix.sys.socket;
        import core.time : Duration, msecs;

        import std.experimental.allocator.mallocator : Mallocator;

        enum maxEvents = 256;
        enum queueSize = 1024;
        enum completionQueueSize = queueSize * 2;
        enum initialCapacity = 1024;

    protected:
        /**
         * Epoll-related
         */
        epoll_event[maxEvents] events;
        int epoll;
        int timeout;

        /** 
         * Queue-related
         */
        FDSet!FDInfo fds;
        FreeList!(Task, Mallocator) tasks;

        // Run queue
        RingBuffer!(Key, queueSize) runQueue;
        RingBuffer!(Event, completionQueueSize) completionQueue;

        /** 
         * Finish the task and set its result
         * Updates pipeline state for the next task
         */
        void completeTask(ref Pipeline pipeline, const Key taskId, ref Task task, long ret)
        {
            const status = ret < 0 ? Status.Type.Error : Status.Type.Success;
            const result = ret < 0 ? errno() : ret;

            // Postpone the blocking task completion until new fd event arrives
            if (result == EINPROGRESS || result == EAGAIN || result == EWOULDBLOCK) {
                pipeline.state = Pipeline.State.notReady;
                return;
            }

            task.status = status;
            task.result = result;

            if (!task.next)
            {
                pipeline.head = 0;
                pipeline.tail = 0;
            }
            else
            {
                pipeline.head = task.next;

                // If the pipeline readiness didn't change, we add the next task
                // to the running queue immediately
                if (pipeline.ready || task.isWrite) {
                    runQueue.put(pipeline.head);
                }
            }

            const event = Event(IO(taskId), task.data.token, Result(status.toResultType, task.result));
            completionQueue.put(event);

            tasks.remove(taskId);
        }

        auto dispatchAccept(ref FDInfo fdi, ref Task task) @trusted
        {
            socklen_t addrLen;
            return accept4(fdi.fd, task.newAddr, &addrLen, SOCK_NONBLOCK);
        }

        /** 
         * Perform socket connection
         *
         * Because connect() may return `EINPROGRESS` on the first run, the funcion will
         * postpone task completion until the next run (or EPOLLERR)
         */
        auto dispatchConnect(ref FDInfo fdi, ref Task task) @trusted
        {
            switch (fdi.write.state)
            {
                case Pipeline.State.error: errno = lastSocketError(fdi.fd); return -1;
                case Pipeline.State.ready: return 0;
                default: return connect(fdi.fd, &task.addr, task.addr.sizeof);
            }
        }

        /** 
         * TODO: Handle EOF (or peer shutdown)
         */
        auto dispatchRead(ref FDInfo fdi, ref Task task) @trusted
        {
            if (fdi.read.state == Pipeline.State.hup) return 0;

            const ret = read(fdi.fd, task.buf.ptr, task.buf.length);
            if (ret == 0) fdi.read.state = Pipeline.State.hup;

            return ret;
        }

        auto dispatchRecv(ref FDInfo fdi, ref Task task) @trusted
        {
            switch (fdi.read.state)
            {
                case Pipeline.State.error: errno = lastSocketError(fdi.fd); return -1;
                case Pipeline.State.hup: return 0;
                default:
                    const ret = recv(fdi.fd, task.buf.ptr, task.buf.length, task.flags);
                    if (ret == 0) fdi.read.state = Pipeline.State.hup;
                    return ret;
            }
        }

        auto dispatchWrite(ref FDInfo fdi, ref Task task) @trusted
        {
            return write(fdi.fd, task.buf.ptr, task.buf.length);
        }

        auto dispatchSend(ref FDInfo fdi, ref Task task) @trusted
        {
            switch (fdi.write.state)
            {
                case Pipeline.State.error: errno = lastSocketError(fdi.fd); return -1;
                default: return send(fdi.fd, task.buf.ptr, cast(int) task.buf.length, task.flags | MSG_NOSIGNAL);
            }
        }

        /**
         * Execute scheduled task
         *
         * TODO: Get rid of switch-case here to achieve cache-friendliness
         */
        void runTask(ref FDInfo fdi, ref Task task, const Key taskId)
        {
            (ref Pipeline pipeline)
            {
                // Just a hack for now
                // TODO: Handle EPOLLRDHUP correctly
                if (pipeline.state == Pipeline.State.hup && task.isWrite) {
                    completeTask(pipeline, taskId, task, EPIPE);
                    return;
                }

                long ret;

                final switch (task.type)
                {
                    case OpType.Accept: ret = dispatchAccept(fdi, task); break;
                    case OpType.Connect: ret = dispatchConnect(fdi, task); break;
                    case OpType.Read: ret = dispatchRead(fdi, task); break;
                    case OpType.Write: ret = dispatchWrite(fdi, task); break;
                    case OpType.Receive: ret = dispatchRecv(fdi, task); break;
                    case OpType.Send: ret = dispatchSend(fdi, task); break;
                }

                completeTask(pipeline, taskId, task, ret);
            }(fdi.pipeline(task));
        }

        /** 
         * Execute all pending tasks from the run queue
         *
         * TODO: Hadle partial reads and writes (when the descriptor is not ready)
         * TODO: Reset pipeline state after errors and HUP (to allow submitting new operations)
         */
        void runTasks()
        {
            for (; !runQueue.empty; runQueue.popFront())
            {
                auto taskId = runQueue.front;
                auto task = tasks.get(taskId);

                if (!task.isNull) {
                    fds.get(task.data.fd, (ref FDInfo fdi) => runTask(fdi, task, taskId));
                }
            }
        }

        /** 
         * Register the file descriptor in Epoll and our internal data structures
         */
        FDInfo register(int fd) @trusted
        {
            epoll_event ev = { events: EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP, data: { fd: fd }};
            epoll_ctl(epoll, EPOLL_CTL_ADD, fd, &ev);
            return FDInfo(fd, Pipeline(Pipeline.State.notReady), Pipeline(Pipeline.State.notReady));
        }

        /** 
         * Unregister the file descriptor in Epoll and clean up internal resources
         */
        void unregister(int fd)
        {
            fds.get(fd, (ref FDInfo fdi) @trusted
            {
                // TODO: Remove all tasks chain for the related FDInfo
                if (fdi.read.head) tasks.remove(fdi.read.head);
                if (fdi.write.head) tasks.remove(fdi.write.head);

                fds.remove(fd);
                epoll_ctl(epoll, EPOLL_CTL_DEL, fd, null);
            });
        }

        /** 
         * Submit the task to the execution pipeline
         */
        IO scheduleTask(const Key taskId) @trusted
        {
            auto task = tasks.get(taskId);
            const fd = task.data.fd;

            // Create new FDInfo if doesn't exist and add it to epoll
            // TODO: Get rid of pointer semantics here
            auto fdi = &fds.require(fd, register(fd));

            /*
             * When we already have the FDInfo in our list, we just need to update
             * pointers for the associated tasks, but we must consider two scenarios:
             *
             * 1. There are no associated tasks for this descriptor
             * 2. There are already some scheduled tasks
             */
            with (fdi.pipeline(task))
            {
                if (!head)
                {
                    head = taskId;
                    tail = taskId;

                    // Immediately put the task to the run queue because the file
                    // descriptor is ready to perform reads or writes
                    if (state == Pipeline.State.ready || task.isWrite) {
                        runQueue.put(taskId);
                    }
                }
                else
                {
                    auto lastTask = tasks.get(tail);

                    if (!lastTask.isNull) {
                        lastTask.next = taskId;
                        tail = taskId;
                    }
                }
            }

            return IO(taskId);
        }

        /** 
         * Process events coming from the epoll socket
         */
        void processEvents(size_t minTasks = 0) @trusted
        {
            auto ret = epoll_wait(epoll, events.ptr, cast(int) events.length, -1);

            // TODO: Gracefully handle epoll_wait errors 
            if (ret <= 0) {
                return;
            }

            foreach (ref ev; events[0 .. ret])
            {
                const fd = ev.data.fd;
                auto fdi = fd in fds;

                // TODO: Get rid of pointers here
                if (!fdi) {
                    epoll_ctl(epoll, EPOLL_CTL_DEL, fd, null);
                    continue;
                }

                // BUG: Prevent scheduling the same task multiple times when receiving duplicate events
                if (ev.events & EPOLLIN)
                {
                    fdi.read.state =
                        ev.events & EPOLLERR ? Pipeline.State.error :
                        ev.events & EPOLLRDHUP ? Pipeline.State.hup : Pipeline.State.ready;

                    if (tasks.has(fdi.read.head)) runQueue.put(fdi.read.head);
                }

                // TODO: Handle partially-completed write operations (and retries in case of EINTR)
                if (ev.events & EPOLLOUT)
                {
                    fdi.write.state =
                        ev.events & EPOLLERR ? Pipeline.State.error :
                        ev.events & EPOLLHUP ? Pipeline.State.hup : Pipeline.State.ready;

                    if (tasks.has(fdi.write.head)) runQueue.put(fdi.write.head);
                }
            }
        }

    public:
        this(size_t minCapacity = initialCapacity) @trusted
        {
            epoll = epoll_create1(0);
            tasks = new typeof(tasks)(minCapacity);
        }

        /** 
         * TODO: Uninitialize containers and buffers
         */
        ~this() @trusted
        {
            close(epoll);
        }

        alias List(Elem...) = Elem;

        static foreach (T; List!(Connect, Accept, Receive, Send, Read, Write))
        {
            IO submit(T op)
            {
                auto taskId = tasks.add(toTask(op));
                return scheduleTask(taskId);
            }
        }

        /** 
         * Cancel IO operation
         *
         * TODO: Remove cancelled operation from every place and fix the chain pointers
         */
        bool cancel(IO op)
        {
            tasks.remove(Key(op));
            return true;
        }

        /** 
         * Execute the tasks and wait for completion events
         *
         * Params:
         *   minTasks = minimum tasks to be completed before wake up from `wait()`
         */
        size_t wait(size_t minTasks = 1)
        {
            if (!tasks.empty)
            {
                runTasks();
                if (length) return tasks.length;

                processEvents(minTasks);
                runTasks();
            }

            return tasks.length;
        }

        void popFront() { completionQueue.popFront(); }

        @property
        {
            size_t length() const { return completionQueue.length; }
            bool empty() const { return completionQueue.empty; }
            Event front() const { return completionQueue.front; }
        }

        /** 
         * TODO: Report errors when registering the descriptor
         */
        void open(int fd)
        {
            register(fd);
        }

        void close(int fd)
        {
            unregister(fd);
            .close(fd);
        }
    }

version(unittest)
{
    import std.algorithm.iteration : map;
    import std.typecons : tuple;
    import std.array : assocArray;

    Socket makeServerSocket(InetAddr addr) @trusted
    {
        const fd = Socket(socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));
        int flags = 1;

        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flags, flags.sizeof);
        bind(fd, cast(sockaddr*) &addr, addr.sizeof);
        listen(fd, 10);

        return fd;
    }

    Socket[2] makeSocketPair(InetAddr serverAddr) @trusted
    {
        const listener = makeServerSocket(serverAddr);
        const client = Socket(socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));

        connect(client, cast(sockaddr*) &serverAddr, serverAddr.sizeof);

        auto clientAddr = InetAddr();
        socklen_t addrLen;
        auto server = accept(listener, cast(sockaddr*) &clientAddr, &addrLen);

        assert(server > 0);

        return [client, Socket(server)];
    }

    alias Comparator = void delegate (ref Event);

    void assertCompletion()(IOEngine io, Comparator[IO] expected)
    {
        assert(!io.empty);
        assert(io.length == expected.length);

        auto results = expected.dup;

        foreach (ref ev; io)
        {
            assert(ev.op in expected, "Unknown event");
            results[ev.op](ev);
            results.remove(ev.op);
        }

        assert(!results.length, "Expected events are not present");
    }

    void assertCompletion(IOEngine io, Event[] expected)
    {
        assert(io.length == expected.length);

        Comparator[IO] results;

        foreach (const ref ev; expected) {
            results[ev.op] = ((Event src) => (ref Event dst) => assert(src == dst))(ev);
        }

        assertCompletion(io, results);
    }
}

/**
 * NOTE: To make the code inherently safe, the read/write buffers, socket addresses and
 *       other data must not be passed as pointers. To achieve this, we should either pass
 *       a callback or some struct/class that will provide the pointer (requires testing)
 */
@("fileReadStd")
unittest
{
    import std.stdio : stdout;

    ubyte[4] buf;
    auto fd = (() @trusted => File(stdout.fileno))();

    with (new EpollEngine())
    {
        submit(Read(fd, buf));
        assert(wait() == 1, "Read operation must be blocking");
        assert(empty);
        assert(length == 0);
    }
}

@("sockConnectAccept")
@trusted unittest
{
    // Server
    const serverAddr = InetAddr("127.0.0.1", 6760);
    const server = makeServerSocket(serverAddr);

    // Client
    auto clientAddr = InetAddr();
    const client = Socket(socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));

    scope(exit) {
        close(client);
        close(server);
    }

    auto io = new EpollEngine();

    with (io)
    {
        const opAccept = submit(Accept(server, &clientAddr.val));
        const opConnect = submit(Connect(client, serverAddr));

        assert(wait() == 0);

        assertCompletion(io, cast(Comparator[IO]) [
            opAccept: (ref Event e) => assert(e.token.tag == 0 && e.result.done && e.result.value > 0),
            opConnect: (ref Event e) => assert(e == Event(opConnect, Token(), Result(Result.Type.Success, 0))),
        ]);
    }
}

@("sockWriteSuccess")
@trusted unittest
{
    Socket[2] sockets;
    assert(0 == socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, cast(int[2]) sockets));
    scope(exit) {
        close(sockets[0]);
        close(sockets[1]);
    }

    const client = sockets[0];
    const server = sockets[1];

    static msg = cast(void[]) "Hello, world";

    auto io = new EpollEngine();

    with (io)
    {
        const opWrite = submit(Write(client, msg));
        assert(wait() == 0);

        assertCompletion(io, [Event(opWrite, Token(), Result(Result.Type.Success, msg.length))]);
    }

    // Check data on the other end of the socket
    ubyte[100] buf;
    assert(msg.length == recv(server, &buf, buf.length, 0));
}

@("sockSendFail")
@trusted unittest
{
    const fd = Socket(socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, IPPROTO_TCP));
    scope(exit) close(fd);

    auto io = new EpollEngine();

    with (io)
    {
        const opSend = submit(Send(fd, cast(void[]) "Test"));
        assert(wait() == 0);

        assertCompletion(io, [Event(opSend, Token(), Result(Result.Type.Error, EPIPE))]);
    }
}

@("sockSendHup")
@trusted unittest
{
    static msg = cast(immutable void[]) "Test1";

    const sockets = makeSocketPair(InetAddr("127.0.0.1", 6767));
    scope(exit) {
        close(sockets[0]);
        close(sockets[1]);
    }

    assert(0 == shutdown(sockets[1], SHUT_RDWR));

    auto io = new EpollEngine();

    with (io)
    {
        const opSend1 = submit(Send(sockets[0], cast(void[]) msg));
        const opSend2 = submit(Send(sockets[0], cast(void[]) "Test2"));
        const opConnect = submit(Connect(sockets[0], InetAddr("127.0.0.1", 6768)));

        assert(wait() == 0);

        assertCompletion(io, [
            Event(opSend1, Token(), Result(Result.Type.Success, msg.length)),
            Event(opSend2, Token(), Result(Result.Type.Error, EPIPE)),
            Event(opConnect, Token(), Result(Result.Type.Error, ECONNABORTED)),
        ]);
    }
}

@("sockRecvSuccess")
@trusted unittest
{
    static msg = "Test1";
    static completed = false;

    Socket[2] sockets;
    scope(exit) {
        close(sockets[0]);
        close(sockets[1]);
    }

    assert(0 == socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, cast(int[2]) sockets));
    assert(msg.length == send(sockets[1], msg.ptr, cast(int) msg.length, 0));

    ubyte[10] buf;

    auto io = new EpollEngine();

    with (io)
    {
        const opRecv = submit(Receive(sockets[0], buf));        
        assert(wait() == 0);
        assertCompletion(io, [Event(opRecv, Token(), Result(Result.Type.Success, msg.length))]);
    }
}

@("sockRecvHup")
unittest
{
    Socket[2] sockets;
    scope(exit) {
        close(sockets[0]);
        close(sockets[1]);
    }

    assert(0 == socketpair(AF_UNIX, SOCK_STREAM | SOCK_NONBLOCK, 0, cast(int[2]) sockets));
    assert(0 == shutdown(sockets[1], SHUT_WR));

    ubyte[10] buf;

    auto io = new EpollEngine();

    with (io)
    {
        const opRecv = submit(Receive(sockets[0], buf));
        const opRead = submit(Read(sockets[0], buf));
        assert(wait() == 0);

        assertCompletion(io, [
            Event(opRecv, Token(), Result(Result.Type.Success, 0)),
            Event(opRead, Token(), Result(Result.Type.Success, 0)),
        ]);
    }
}

/+
Test cases:

1. Connect
    - Success
    - Failure
    - Connect after fail
2. Accept
    - Success
    - Failure
    - Accept after fail
3. Read/Recv
    - EOF/HUP handling: all subsequent scheduled reads must fail with EOF
    - EINTR handling (retry read)
    - Failure
4. Write/Send
    - Success
    - Fail
    - HUP handling: all subsequent scheduled writes (except Connect) must fail
5. Chained operations
    - Connect failed: finalize all Reads and Writes with faliure
+/
