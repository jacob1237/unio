module unio.engine.epoll;

@safe:// @nogc:

public import unio.engine;

private:
    import core.sys.posix.netinet.in_;
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
            bool ready() { return state == State.ready; }
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

        @property bool isWrite() const
        {
            return cast(bool) (type & 0xF0);
        }
    }

    /** 
     * Convert operation data to a Task entry
     */
    Task toTask(Op)(Op op) @trusted
    {
        auto entry = Task(
            mixin("OpType." ~ Op.stringof),
            Status.Type.Pending,
            Task.Data(cast(size_t) op.fd, op.key, op.cb)
        );

        static if (__traits(hasMember, Op, "buf")) {
            entry.buf = op.buf;
        }
        else static if (__traits(hasMember, Op, "addr")) {
            entry.addr = op.addr;
        }
        else static if (__traits(hasMember, Op, "newAddr")) {
            entry.newAddr = cast(sockaddr*) op.newAddr;
        }

        static if (__traits(hasMember, Op, "flags")) {
            entry.flags = op.flags;
        }

        return entry;
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
     * TODO: Add support for the `Wait` operation
     * TODO: Distinguish different types of file descriptors
     * TODO: Add operation chaining
     * TODO: Add support for timers
     * TODO: Add support for async file operations (via AIO/io_submit or a dedicated thread pool)
     * TODO: Handle connection errors both for read/write operations
     * TODO: Rename read/write task types to input/output
     * TODO: Make task containers more flexible and efficient
     * TODO: Edge case: epoll may notify readiness for send(), but EAGAIN will be returned: https://habr.com/ru/post/416669/#comment_18865881
     * TODO: Add unit tests
     * TODO: Handle SIGPIPE correctly when using `write()`: https://stackoverflow.com/a/18963142/7695184
     * TODO: Handle setsockopt() `SO_RCVTIMEO` and `SO_SNDTIMEO` (check how do they react)
     * TODO: Call epoll_ctl(EPOLL_CTL_DEL) when there are no tasks for a file descriptor for a long time
     */
    class EpollEngine : IOEngine
    {
        import core.stdc.errno;
        import core.sys.linux.epoll;
        import core.sys.posix.arpa.inet;
        import core.sys.posix.unistd;
        import core.sys.posix.sys.socket;
        import core.time : Duration, msecs;

        import std.experimental.allocator.mallocator : Mallocator;

        enum maxEvents = 256;
        enum queueSize = 1024;
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

                // If the readiness pipeline didn't change, we add the next task
                // to the running queue immediately
                if (pipeline.ready || task.isWrite) {
                    runQueue.put(pipeline.head);
                }
            }

            if (task.data.cb !is null) {
                task.data.cb(this, IO(taskId));
            }
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
         * TODO: handle EOF (or peer shutdown)
         */
        auto dispatchRead(ref FDInfo fdi, ref Task task) @trusted
        {
            const ret = read(fdi.fd, task.buf.ptr, cast(int) task.buf.length);
            if (ret == 0) fdi.read.state = Pipeline.State.hup;

            return ret;
        }

        auto dispatchWrite(ref FDInfo fdi, ref Task task) @trusted
        {
            return write(fdi.fd, task.buf.ptr, cast(int) task.buf.length);
        }

        auto dispatchRecv(ref FDInfo fdi, ref Task task) @trusted
        {
            return recv(fdi.fd, task.buf.ptr, cast(int) task.buf.length, task.flags);
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
         * Execute all pending tasks from the run queue
         *
         * TODO: Get rid of switch-case here to achieve cache-friendliness
         * TODO: Hadle partial reads and writes (when the descriptor is not ready)
         * TODO: Reset pipeline state after errors and HUP (to allow submit new operations)
         */
        void runTasks() @trusted
        {
            for (; !runQueue.empty; runQueue.popFront())
            {
                auto taskId = runQueue.front;
                auto task = tasks.get(taskId);

                if (task.isNull) {
                    continue;
                }

                if (auto fdi = task.data.fd in fds)
                {
                    auto pipeline = fdi.pipeline(task);

                    // Just a hack for now
                    // TODO: Handle EPOLLRDHUP correctly
                    if (pipeline.state == Pipeline.State.hup && task.isWrite) {
                        completeTask(pipeline, taskId, task, EPIPE);
                        continue;
                    }

                    long ret;

                    final switch (task.type)
                    {
                        case OpType.Accept: ret = dispatchAccept(*fdi, task); break;
                        case OpType.Connect: ret = dispatchConnect(*fdi, task); break;
                        case OpType.Read: ret = dispatchRead(*fdi, task); break;
                        case OpType.Write: ret = dispatchWrite(*fdi, task); break;
                        case OpType.Receive: ret = dispatchRecv(*fdi, task); break;
                        case OpType.Send: ret = dispatchSend(*fdi, task); break;
                    }

                    completeTask(pipeline, taskId, task, ret);
                }
            }
        }

        /** 
         * Submit the task to the execution pipeline
         */
        IO scheduleTask(const Key taskId) @trusted
        {
            auto task = tasks.get(taskId);
            const fd = task.data.fd;

            // Create new FDInfo if doesn't exist and add it to epoll
            auto fdi = &fds.require(fd, {
                epoll_event ev = { events: EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP, data: { fd: fd }};
                epoll_ctl(epoll, EPOLL_CTL_ADD, fd, &ev);

                return FDInfo(fd, Pipeline(Pipeline.State.notReady), Pipeline(Pipeline.State.notReady));
            }());

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
        void processEvents() @trusted
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
         * Check status of some IO operation
         *
         * Please note that when the status is not Pending, the task entry
         *  will be automatically cleaned up after calling this function
         */
        Status status(IO op)
        {
            const key = Key(op);
            const task = tasks.get(key);
            const result = Status(task.status, task.result);

            if (task.status != Status.Type.Pending) {
                tasks.remove(key);
            }

            return result;
        }

        /** 
         * Receive OS events and perform loop tasks
         * TODO: Get rid of duplicate calls to `runTasks` and `processSumbissionQueue`
         */
        size_t process()
        {
            if (tasks.empty) {
                return 0;
            }

            runTasks();
            processEvents();
            runTasks();

            return tasks.length;
        }
    }

/**
 * NOTE: To make the code inherently safe, the read/write buffers, socket addresses and
 *       other data must not be passed as pointers. To achieve this, we should either pass
 *       a callback or some struct/class that will provide the pointer (requires testing)
 */
@("fdStdRead")
unittest
{
    import std.stdio : stdout;

    void cb(IOEngine io, IO op) { io.status(op); }

    ubyte[4] buf;
    auto io = new EpollEngine();
    auto fd = (() @trusted => File(stdout.fileno))();

    io.submit(Read(fd, 0, &cb, buf));
    assert(1 == io.process(), "Read operation must be blocking");
}

@("sockConnectAccept")
@trusted unittest
{
    import std.stdio;
    import core.stdc.errno;

    static bool accepted;
    void serverCb(IOEngine io, IO op) { accepted = true; io.status(op); }

    static bool connected;
    void clientCb(IOEngine io, IO op) { connected = true; io.status(op); }

    // Server
    const server = Socket(socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));
    const serverAddr = InetAddr("127.0.0.1", 6767);

    int flags = 1;
    setsockopt(server, SOL_SOCKET, SO_REUSEADDR, &flags, int.sizeof);
    bind(server, cast(sockaddr*) &serverAddr, serverAddr.sizeof);
    listen(server, 10);

    // Client
    const client = Socket(socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0));
    auto clientAddr = InetAddr();

    auto io = new EpollEngine();
    io.submit(Accept(server, 0, &serverCb, &clientAddr.val));
    io.submit(Connect(client, 0, &clientCb, serverAddr));
    io.process();

    assert(connected);
    assert(accepted);
}
