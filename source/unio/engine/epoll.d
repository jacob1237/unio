module unio.engine.epoll;

@safe:

public import unio.engine;

private:
    import core.stdc.errno;
    import core.sys.posix.netinet.in_;
    import core.sys.posix.unistd;
    import std.experimental.allocator.mallocator : Mallocator;
    import unio.primitives;
    import unio.engine.modules.timers;

    enum SOCK_NONBLOCK = 0x800;
    extern (C) int accept4(int, sockaddr*, socklen_t*, int);

    alias Key = ArrayPool!(Task, Mallocator).Key;
    alias Timer = Timers!(Key, Mallocator).Timer;

    /** 
     * Operation type enum.
     * All read tasks have lowest bits set in opposed to write-related.
     */
    enum OpType : ubyte {
        // Read tasks
        Receive = 0x01,
        Read = 0x02,
        Accept = 0x03,
        Timeout = 0x04,

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
        enum Status : ubyte { ready, notReady, error, hup }

        Status status;
        Queue!(Key).State state;

        @property
        {
            bool ready() { return status != Status.notReady; }
            bool error() { return status == Status.error; }
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

        ref Pipeline pipeline(const scope ref Task t) return { return t.isWrite ? write : read; }
    }

    /** 
     * Internal EPoll engine task item.
     *
     * Contains both operation data and arguments, as well as
     * pointers to the next socket operation (for easy search).
     */
    struct Task
    {
        struct Data { mixin Operation!int; }

        OpType type;
        Status.Type status;
        Data data;

        Queue!(Key).Entry pipelineEntry;
        Queue!(Key).Entry queueEntry;

        // Params
        union
        {
            void[] buf;
            sockaddr addr;
            sockaddr* newAddr;
            Timer timer;
        }

        size_t result;
        int flags;

        @property bool isWrite() scope const { return cast(bool) (type & 0xF0); }
        Result toResult() scope { return Result(toResultType(status), result); }
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

    /** 
    TODO: Make the `events` range unified across all selectors
    */
    struct Epoll(size_t MaxEvents)
    {
        import core.sys.linux.epoll;

        enum
        {
            IN = EPOLLIN,
            OUT = EPOLLOUT,
            ERR = EPOLLERR,
            HUP = EPOLLHUP,
            RDHUP = EPOLLRDHUP,
        }

        private int epfd;

        public:
            epoll_event[MaxEvents] events;

            @disable this();
            @disable this(this);

            this(int flags) @trusted
            {
                epfd = epoll_create1(flags);
            }

            ~this()
            {
                close(epfd);
            }

            int add(int fd) @trusted
            {
                epoll_event ev = {
                    events: EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP,
                    data: { fd: fd }
                };

                return epoll_ctl(epfd, EPOLL_CTL_ADD, fd, &ev);
            }

            int remove(int fd) @trusted
            {
                return epoll_ctl(epfd, EPOLL_CTL_DEL, fd, null);
            }

            int select(int timeout = 0) @trusted
            {
                return epoll_wait(epfd, events.ptr, MaxEvents, timeout);
            }
    }

public:
    /** 
     * Epoll IO engine implementation
     *
     * TODO: Add `Cancel` support for all operations on a specific file descriptor
     * TODO: Add support for the `Wait` operation
     * TODO: Distinguish different types of file descriptors
     * TODO: Add support for async file operations (via AIO/io_submit or a dedicated thread pool)
     * TODO: Parallelize IO operations (thread pool?)
     * TODO: Handle connection errors both for read/write operations
     * TODO: Rename read/write task types to input/output
     * TODO: Edge case: epoll may notify readiness for send(), but EAGAIN will be returned: https://habr.com/ru/post/416669/#comment_18865881
     * TODO: Handle setsockopt() `SO_RCVTIMEO` and `SO_SNDTIMEO` (check how do they react)
     * TODO: Call epoll_ctl(EPOLL_CTL_DEL) when there are no tasks for a file descriptor for a long time
     * TODO: Handle vectorized I/O (iovec)
     * TODO: Handle EPOLLPRI to read the OOB data (support it in Recv/Send calls)
     */
    class EpollEngine : IOEngine
    {
        import core.sys.posix.arpa.inet;
        import core.sys.posix.sys.socket;
        import core.time : Duration, msecs;
        import std.typecons : NullableRef;

        enum maxEvents = 256;
        enum initialCapacity = 1024;

    protected:
        struct IOResult
        {
            private alias This = typeof(this);

            Status.Type type;
            size_t value;
            alias value this;

            static
            {
                auto error(size_t code) { return This(Status.Type.Error, code); }
                auto success(size_t code) { return This(Status.Type.Success, code); }
                auto from(long ret)
                {
                    return ret < 0 ? This(Status.Type.Error, errno) : This(Status.Type.Success, ret);
                }
            }

            @property
            {
                bool failed() const { return type == Status.Type.Error; }
                bool inProgress() const
                {
                    return failed && (value == EINPROGRESS || value == EWOULDBLOCK || value == EAGAIN);
                }
            }
        }

        Epoll!maxEvents selector;

        /*
        Timer related
        */
        Timers!(Key, Mallocator) timers;
        TimerFd clock;

        /** 
         * Queue-related
         */
        Table!(FDInfo, initialCapacity, Mallocator) fds;
        ArrayPool!(Task, Mallocator) tasks;

        Queue!(Key).Range runQueue;
        Queue!(Key).Range completionQueue;
        Queue!(Key) pipelines;

        /**
        Linked list node resolver for the run queue
        */
        auto resolveQueueEntry(Key k) nothrow @nogc
        {
            return NullableRef!(Queue!(Key).Entry)(
                tasks.take(k, (ref Task t) => &t.queueEntry, () => null)
            );
        }

        auto resolvePipelineEntry(Key k) nothrow @nogc
        {
            return NullableRef!(Queue!(Key).Entry)(
                tasks.take(k, (ref Task t) => &t.pipelineEntry, () => null)
            );
        }

        /**
        The function used by the `Timers` struct to resolve the Timer data from the given key.
        This is required because we store timers on top of the `Task` data.
        */
        auto resolveTimer(Key k) nothrow @nogc
        {
            return NullableRef!Timer(tasks.take(k, (ref Task t) => &t.timer, () => null));
        }

        /**
        The rules of cancelling a timeout are a bit different than for a regular task:

        because timers are not using the pipeline in the same way, we need to manually re-arm
        the timer clock if after removing the current timer, the pipeline head changes
        */
        void cancelTimeout(scope ref FDInfo fdi, scope ref Task task)
        {
            timers.remove(task.timer);

            with (pipelines.make(fdi.read.state))
            {
                if (timers.empty || timers.front == front) return;

                const nextId = timers.front;

                reset();
                put(nextId);

                tasks.take(nextId, (scope ref Task next) => clock.arm(next.timer));
            }
        }

        /** 
        Submit the specified task to the run queue
        */
        void enqueueTask(Key taskId)
        {
            tasks.take(taskId, (scope ref Task t)
            {
                if (t.status == Status.Type.Pending)
                {
                    t.status = Status.Type.Running;
                    runQueue.put(taskId);
                }
            });
        }

        /**
        Cancel the task by its ID.
        If the cancellation was successful, the task will be moved to the completion queue
        and a user may expect to get an error result with ECANCELED

        TODO: Ensure that completed tasks do not exist in this state inside the pipeline to prevent infinite cancellation loop in `unregister()``
        */
        bool cancelTask(Key taskId)
        {
            return tasks.take(
                taskId,
                (scope ref Task task)
                {
                    switch (task.status)
                    {
                        case Status.Type.Running:
                            runQueue.remove(task.queueEntry);
                            goto case;

                        case Status.Type.Pending:
                            fds.take(task.data.fd, (scope ref FDInfo fdi) @safe
                            {
                                if (task.type == OpType.Timeout) cancelTimeout(fdi, task);
                                else pipelines
                                    .make(fdi.pipeline(task).state)
                                    .remove(task.pipelineEntry);
                            });

                            task.status = Status.Type.Error;
                            task.result = ECANCELED;

                            completionQueue.put(taskId);
                            return true;

                        default:
                            return false;
                    }
                },
                () => false
            );
        }

        /** 
         * Finish the task and set its result
         * Updates pipeline state for the next task
         */
        void completeTask(ref Pipeline pipeline, in Key taskId, ref Task task, in IOResult res)
        {
            task.status = res.type;
            task.result = res.value;

            with (pipelines.make(pipeline.state))
            {
                popFront();

                // If the pipeline readiness didn't change, we add the next task
                // to the running queue immediately
                if (!empty && (pipeline.ready || task.isWrite)) {
                    enqueueTask(front);
                }
            }

            completionQueue.put(taskId);
        }

        IOResult dispatchAccept(ref FDInfo fdi, ref Task task) @trusted
        {
            socklen_t addrLen;
            return IOResult.from(accept4(fdi.fd, task.newAddr, &addrLen, SOCK_NONBLOCK));
        }

        /** 
         * Perform socket connection
         *
         * Because connect() may return `EINPROGRESS` on the first run, the funcion will
         * postpone task completion until the next run (or EPOLLERR)
         */
        IOResult dispatchConnect(ref FDInfo fdi, ref Task task) @trusted
        {
            switch (fdi.write.status)
            {
                case Pipeline.Status.error: return IOResult.error(fdi.fd.lastSocketError);
                case Pipeline.Status.ready: return IOResult.success(0);
                default: return IOResult.from(connect(fdi.fd, &task.addr, task.addr.sizeof));
            }
        }

        /** 
         * TODO: Handle EOF (or peer shutdown)
         */
        IOResult dispatchRead(ref FDInfo fdi, ref Task task) @trusted
        {
            if (fdi.read.status == Pipeline.Status.hup) return IOResult.success(0);

            const ret = read(fdi.fd, task.buf.ptr, task.buf.length);
            if (ret == 0) fdi.read.status = Pipeline.Status.hup;

            return IOResult.from(ret);
        }

        IOResult dispatchRecv(ref FDInfo fdi, ref Task task) @trusted
        {
            switch (fdi.read.status)
            {
                case Pipeline.Status.error: return IOResult.error(fdi.fd.lastSocketError);
                case Pipeline.Status.hup: return IOResult.success(0);
                default:
                    const ret = recv(fdi.fd, task.buf.ptr, task.buf.length, task.flags);
                    if (ret == 0) fdi.read.status = Pipeline.Status.hup;
                    return IOResult.from(ret);
            }
        }

        IOResult dispatchWrite(ref FDInfo fdi, ref Task task) @trusted
        {
            return IOResult.from(write(fdi.fd, task.buf.ptr, task.buf.length));
        }

        IOResult dispatchSend(ref FDInfo fdi, ref Task task) @trusted
        {
            switch (fdi.write.status)
            {
                case Pipeline.Status.error: return IOResult.error(fdi.fd.lastSocketError);
                default: return IOResult.from(
                    send(fdi.fd, task.buf.ptr, cast(int) task.buf.length, task.flags | MSG_NOSIGNAL)
                );
            }
        }

        IOResult dispatchTimeout(scope ref FDInfo fdi, scope ref Task task)
        {
            timers.remove(task.timer);
            const nextId = timers.front;

            tasks.take(nextId, (scope ref Task next)
            {
                pipelines.make(fdi.read.state).put(nextId);

                if (!next.timer.expired)
                {
                    fdi.read.status = Pipeline.Status.notReady;
                    clock.arm(next.timer);
                }
            });

            return IOResult.success(0);
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
                if (pipeline.status == Pipeline.Status.hup && task.isWrite) {
                    return completeTask(pipeline, taskId, task, IOResult.error(EPIPE));
                }

                IOResult res;

                final switch (task.type)
                {
                    case OpType.Accept: res = dispatchAccept(fdi, task); break;
                    case OpType.Connect: res = dispatchConnect(fdi, task); break;
                    case OpType.Read: res = dispatchRead(fdi, task); break;
                    case OpType.Write: res = dispatchWrite(fdi, task); break;
                    case OpType.Receive: res = dispatchRecv(fdi, task); break;
                    case OpType.Send: res = dispatchSend(fdi, task); break;
                    case OpType.Timeout: res = dispatchTimeout(fdi, task); break;
                }

                if (!res.inProgress) return completeTask(pipeline, taskId, task, res);

                // Postpone the execution until the file descriptor is ready again
                task.status = Status.Type.Pending;
                pipeline.status = Pipeline.Status.notReady;
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
            while (!runQueue.empty)
            {
                auto taskId = runQueue.front;
                runQueue.popFront();

                tasks.take(taskId, (ref Task task) =>
                    fds.take(task.data.fd, (ref FDInfo fdi) =>
                        runTask(fdi, task, taskId)));
            }
        }

        /** 
         * Register the file descriptor in Epoll and our internal data structures
         */
        FDInfo register(int fd)
        {
            selector.add(fd);
            return FDInfo(fd, Pipeline(Pipeline.Status.notReady), Pipeline(Pipeline.Status.notReady));
        }

        /** 
         * Unregister the file descriptor in Epoll and clean up internal resources
         */
        void unregister(int fd)
        {
            fds.take(fd, (ref FDInfo fdi)
            {
                foreach (taskId; pipelines.make(fdi.read.state)) cancelTask(taskId);
                foreach (taskId; pipelines.make(fdi.write.state)) cancelTask(taskId);

                fds.remove(fd);
                selector.remove(fd);
            });
        }

        /** 
         * Submit the task to the execution pipeline
         */
        IO scheduleTask(const Key taskId, ref Task task)
        {
            const fd = task.data.fd;

            /*
            When we already have the FDInfo in our list, we just need to insert
            the task to the pipeline, but we must consider two scenarios:

            1. There are no associated tasks for this descriptor
            2. There are already some scheduled tasks

            In the first case we need to run the task immediately (if the fd is ready)
            */
            with (fds.require(fd, register(fd)).pipeline(task))
            {
                with (pipelines.make(state))
                {
                    put(taskId);

                    if (length == 1 && (status == Pipeline.Status.ready || task.isWrite)) {
                        enqueueTask(taskId);
                    }
                }
            }

            return IO(taskId);
        }

        /** 
        Process events coming from the epoll socket
        
        TODO: Use epoll_pwait2() for a sub-millisecond Timeout implementation
        */
        void processEvents(size_t minTasks = 0)
        {
            // TODO: Gracefully handle epoll_wait errors
            with (selector)
            {
                auto ret = select(-1);
                if (ret <= 0) return;

                foreach (ref ev; events[0 .. ret])
                {
                    const fd = ev.data.fd;

                    fds.take(
                        fd,
                        (ref FDInfo fdi)
                        {
                            if (ev.events & IN)
                            {
                                fdi.read.status =
                                    ev.events & ERR ? Pipeline.Status.error :
                                    ev.events & RDHUP ? Pipeline.Status.hup : Pipeline.Status.ready;

                                with (pipelines.make(fdi.read.state)) {
                                    if (!empty) enqueueTask(front);
                                }
                            }

                            // TODO: Handle partially-completed write operations (and retries in case of EINTR)
                            if (ev.events & OUT)
                            {
                                fdi.write.status =
                                    ev.events & ERR ? Pipeline.Status.error :
                                    ev.events & HUP ? Pipeline.Status.hup : Pipeline.Status.ready;

                                with (pipelines.make(fdi.write.state)) {
                                    if (!empty) enqueueTask(front);
                                }    
                            }
                        },
                        () => remove(fd)
                    );
                }
            }
        }

    public:
        this()
        {
            this(initialCapacity);
        }

        this(size_t minCapacity)
        {
            selector = typeof(selector)(0);
            tasks = typeof(tasks)(minCapacity);
            fds = typeof(fds)(initialCapacity);

            // Initialize work queues
            auto queue = Queue!(Key)(&resolveQueueEntry);

            runQueue = queue.make();
            completionQueue = queue.make();
            pipelines = Queue!(Key)(&resolvePipelineEntry);

            // Initialize the timers subsystem
            timers = typeof(timers)(tasks.capacity, &resolveTimer);
            clock = typeof(clock).make();
            fds[clock.fd] = register(clock.fd);
        }

        alias List(Elem...) = Elem;

        static foreach (T; List!(Connect, Accept, Receive, Send, Read, Write))
        {
            IO submit(T op)
            {
                auto taskId = tasks.put(toTask(op));

                // TODO: Check for pool errors when inserting
                return tasks.take(
                    taskId,
                    (ref Task task) => scheduleTask(taskId, task),
                    () => IO(0)
                );
            }
        }

        IO submit(Timeout op)
        {
            Task task = {
                type: OpType.Timeout,
                data: { fd: clock.fd },
                timer: Timer(op.dur),
            };

            const taskId = tasks.put(task);

            timers.put(taskId);

            // If after inserting a new timer, the front timer changes,
            // we need to re-arm the clock
            fds.take(clock.fd, (scope ref FDInfo fdi)
            {
                with (pipelines.make(fdi.read.state))
                {
                    if (front == timers.front) return;

                    reset();
                    put(timers.front);

                    clock.arm(task.timer);
                }
            });

            return IO(taskId);
        }

        /** 
        Cancel IO operation
        */
        bool cancel(IO op)
        {
            return cancelTask(cast(Key) op);
        }

        /** 
         * Execute the tasks and wait for completion events
         *
         * Params:
         *   minTasks = minimum tasks to be completed before wake up from `wait()`
         */
        size_t wait(size_t minTasks = 1)
        {
            if (pending)
            {
                runTasks();
                if (length) return pending;

                // TODO: Repeat processEvents() until there are some tasks to run or certain conditions met (timeout, minTasks)
                processEvents(minTasks);
                runTasks();
            }

            return pending;
        }

        void popFront()
        {
            const taskId = completionQueue.front;

            completionQueue.popFront();
            tasks.remove(taskId);
        }

        @property
        {
            size_t pending() const { return tasks.length - completionQueue.length; }
            size_t length() const { return completionQueue.length; }
            bool empty() const { return completionQueue.empty; }

            Event front()
            {
                const taskId = completionQueue.front;

                return tasks.take(
                    taskId,
                    (scope ref Task t) => Event(IO(taskId), t.data.token, t.toResult),
                    () => Event()
                );
            }
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
        import std.format : format;

        assert(io.length == expected.length);

        Comparator[IO] results;

        foreach (const ref ev; expected)
        {
            results[ev.op] = ((Event src)
                => (ref Event dst) => assert(
                    src == dst,
                    "Invalid completion entry: expected <%s>, got <%s>".format(src, dst)
                ))(ev);
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
        () @trusted { submit(Read(fd, buf)); }();
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
        const opRecv = (() @trusted => submit(Receive(sockets[0], buf)))();
        const opRead = (() @trusted => submit(Read(sockets[0], buf)))();
        assert(wait() == 0);

        assertCompletion(io, [
            Event(opRecv, Token(), Result(Result.Type.Success, 0)),
            Event(opRead, Token(), Result(Result.Type.Success, 0)),
        ]);
    }
}

@("epollSubmitTimeout")
unittest
{
    import core.time;
    import std.math;

    static immutable dur1 = 1.msecs;
    static immutable dur2 = 10.msecs;
    static immutable drift = 200.usecs;

    void assertDuration(Duration elapsed, Duration expected)
    {
        assert(abs(elapsed - expected) <= drift);
    }

    auto io = new EpollEngine();

    with (io)
    {
        // Schedule timers in a reverse order to ensure the correct execution
        const opTimeout2 = submit(Timeout(dur2));
        const opTimeout1 = submit(Timeout(dur1));

        const start1 = MonoTime.currTime;
        const remaining1 = wait();
        const elapsed1 = MonoTime.currTime - start1;

        assert(remaining1 == 1);
        assertDuration(elapsed1, dur1);
        assertCompletion(io, [Event(opTimeout1, Token(), Result(Result.Type.Success, 0))]);

        const start2 = MonoTime.currTime;
        const remaining2 = wait();
        const elapsed2 = MonoTime.currTime - start2;

        assert(remaining2 == 0);
        assertDuration(elapsed2, dur2 - dur1);
        assertCompletion(io, [Event(opTimeout2, Token(), Result(Result.Type.Success, 0))]);
    }
}

@("epollCancel")
unittest
{
    import core.time;
    import core.thread;
    import std.stdio : stdout;

    ubyte[4] buf;
    auto fd = (() @trusted => File(stdout.fileno))();

    auto io = new EpollEngine();

    with (io)
    {
        () @trusted
        {
            const opRead1 = submit(Read(fd, buf));
            const opRead2 = submit(Read(fd, buf));

            assert(wait() == 2);

            assert(cancel(opRead2));
            assert(pending == 1);
            assert(length == 1);
            assertCompletion(io, [Event(opRead2, Token(), Result(Result.Type.Error, ECANCELED))]);

            assert(cancel(opRead1));
            assert(length == 1);
            assert(pending == 0);
            assertCompletion(io, [Event(opRead1, Token(), Result(Result.Type.Error, ECANCELED))]);

            // Test timeout cancellation
            const opTime1 = submit(Timeout(1.msecs));
            const opTime2 = submit(Timeout(2.msecs));
            const opTime3 = submit(Timeout(1.seconds));

            assert(cancel(opTime1));
            assert(pending == 2);
            assert(length == 1);
            assertCompletion(io, [Event(opTime1, Token(), Result(Result.Type.Error, ECANCELED))]);

            // By this time the 2ms timer opTime2 must be correctly completed
            Thread.sleep(5.msecs);

            const ret = wait();
            assert(ret == 1);
            assert(pending == 1);
            assert(length == 1);
            assertCompletion(io, [Event(opTime2, Token(), Result(Result.Type.Success, 0))]);

            assert(cancel(opTime3));
            assert(pending == 0);
        }();
    }
}
