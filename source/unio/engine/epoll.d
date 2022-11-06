module unio.engine.epoll;

@safe @nogc:

public import unio.engine;

private:
    import std.stdio;

    import core.sys.posix.netinet.in_;
    import unio.primitives.pool : FreeList, Key;

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
     * File descriptor state
     */
    struct FDInfo
    {
        struct State
        {
            bool ready;
            Key head; // First task in the queue
            Key tail; // Last task in the queue
        }

        int fd;
        State read;
        State write;

        State getState(const ref Task t)
        {
            return t.isWrite ? this.write : this.read;
        }

        /** 
         * Return state struct depending on the passed task type
         */
        void setState(const ref Task t, const State newState)
        {
            if (t.isWrite) {
                write = newState;
            }
            else {
                read = newState;
            }
        }
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
        size_t prev;
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
     * Convert operation data to a squeue entry
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

public:
    /** 
     * Epoll IO engine implementation
     *
     * TODO: Add support for the `Wait` operation
     * TODO: Handle connection errors both for read/write operations
     * TODO: Rename read/write task types to input/output
     * TODO: Make task containers more flexible and efficient
     * TODO: Edge case: epoll may notify readiness for send(), but EAGAIN will be returned: https://habr.com/ru/post/416669/#comment_18865881
     */
    class EpollEngine : IOEngine
    {
        import core.stdc.errno : errno, EAGAIN, EINPROGRESS, EWOULDBLOCK, ECONNRESET;
        import core.sys.linux.epoll;
        import core.sys.posix.sys.socket;
        import core.sys.posix.unistd : closefd = close;
        import core.time : Duration, msecs;
        import std.experimental.allocator.mallocator : Mallocator;
        import unio.primitives.fdset : FDSet;
        import unio.primitives.queue : RingBuffer;

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

        // Submission queue
        RingBuffer!(Key, queueSize) squeue;

        // Run queue
        RingBuffer!(Key, queueSize) rqueue;

        /** 
         * Finish the task and set its result
         * Updates descriptor state for the next task
         */
        void completeTask(
            ref FDInfo fdi,
            const Key taskId,
            ref Task task,
            const Status.Type status,
            const long result
        ) @trusted {
            auto state = fdi.getState(task);

            // Skip the blocking task until new fd readiness event arrives
            if (status == Status.type.Error &&
                (result == EINPROGRESS || result == EAGAIN || result == EWOULDBLOCK)
            ) {
                task.result = result;

                state.ready = false;
                fdi.setState(task, state);
                return;
            }

            task.status = status;
            task.result = result;

            with (task)
            {
                if (!next) {
                    state.head = 0;
                    state.tail = 0;
                }
                else
                {
                    state.head = next;

                    // If the readiness state didn't change, we add the next task
                    // to the running queue immediately
                    if (state.ready || task.isWrite) {
                        rqueue.put(state.head);
                    }
                }

                fdi.setState(task, state);

                if (data.cb != data.cb.init) {
                    data.cb(this, IO(taskId));
                }
            }
        }

        /** 
         * Run specific task from the queue
         *
         * TODO: Get rid of switch-case here to achieve cache-friendliness
         * TODO: Hadle partial reads and writes (when the descriptor is not ready)
         */
        void runTask(ref FDInfo fdi, const Key taskId, ref Task task) @trusted
        {
            import core.sys.posix.unistd : read, write;
            import core.sys.posix.arpa.inet : send, recv, accept, connect;

            long ret;

            with (task)
            {
                auto fd = data.fd;

                final switch (type)
                {
                    case OpType.Accept:
                        socklen_t addrLen;
                        ret = accept4(fd, newAddr, &addrLen, SOCK_NONBLOCK);
                        break;

                    case OpType.Connect:
                        // Complete a half-processed connect() if the last retry failed
                        if (result > 0)
                        {
                            result = result != EINPROGRESS ? result : 0;
                            status = result ? Status.Type.Error : Status.Type.Success;

                            completeTask(fdi, taskId, task, status, result);
                            return;
                        }

                        ret = connect(fd, &addr, addr.sizeof);
                        break;

                    case OpType.Read:
                        ret = read(fd, buf.ptr, cast(int) buf.length);
                        break;

                    case OpType.Write:
                        ret = write(fd, buf.ptr, cast(int) buf.length);
                        break;

                    case OpType.Receive:
                        ret = recv(fd, buf.ptr, cast(int) buf.length, flags);
                        break;

                    case OpType.Send:
                        ret = send(fd, buf.ptr, cast(int) buf.length, flags | MSG_NOSIGNAL);
                        break;
                }
            }

            auto status = ret < 0 ? Status.Type.Error : Status.Type.Success;
            auto result = ret < 0 ? errno() : ret;

            completeTask(fdi, taskId, task, status, result);
        }

        /** 
         * Prepare a submitted task for execution
         */
        void processSubmissionEntry(const Key taskId, ref Task task) @trusted
        {
            const fd = task.data.fd;

            // Create new FDInfo if doesn't exist and add it to epoll
            auto fdi = &fds.require(fd,
            {
                epoll_event ev = {
                    events: EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP,
                    data: { fd: fd }
                };

                epoll_ctl(epoll, EPOLL_CTL_ADD, fd, &ev);

                return FDInfo(fd, FDInfo.State(false), FDInfo.State(true));
            }());

            /*
             * When we already have the fd state in our list, we just need to update
             * pointers for the associated tasks, but we must consider two scenarios:
             *
             * 1. There are no associated tasks for this descriptor
             * 2. There are already some scheduled tasks
             */
            auto state = fdi.getState(task);

            if (!state.head)
            {
                state.head = taskId;
                state.tail = taskId;

                // Immediately put the task to the run queue because the file
                // descriptor is ready to perform reads or writes
                if (state.ready || task.isWrite) {
                    rqueue.put(taskId);
                }
            }
            else
            {
                auto lastTask = tasks.get(state.tail);

                if (!lastTask.isNull) {
                    lastTask.next = taskId;
                    task.prev = state.tail;
                    state.tail = taskId;
                }
            }

            fdi.setState(task, state);
        }

        /** 
         * Handle all newly added tasks from the user submission queue
         */
        void processSubmissionQueue()
        {
            while (!squeue.empty)
            {
                const taskId = squeue.front;
                auto task = tasks.get(taskId);

                // Skip cancelled tasks
                if (!task.isNull) {
                    processSubmissionEntry(taskId, task);
                }

                squeue.popFront();
            }
        }

        /** 
         * Process events coming from the epoll socket
         */
        void processEpollEvents() @trusted
        {
            auto ret = epoll_wait(epoll, events.ptr, cast(int) events.length, -1);

            // TODO: Gracefully handle epoll_wait errors 
            if (ret <= 0) {
                return;
            }

            // Schedule tasks associated with epoll events for execution
            foreach (ref ev; events[0 .. ret])
            {
                const fd = ev.data.fd;
                auto fdi = fd in fds;

                // Remove file descriptors whose state isn't tracked by the engine
                if (!fdi) {
                    epoll_ctl(epoll, EPOLL_CTL_DEL, fd, null);
                    continue;
                }

                if (ev.events & EPOLLRDHUP || ev.events & EPOLLHUP)
                {
                    auto readTask = tasks.get(fdi.read.head);
                    auto writeTask = tasks.get(fdi.write.head);

                    if (!readTask.isNull) {
                        completeTask(*fdi, fdi.read.head, readTask, Status.Type.Error, ECONNRESET);
                    }

                    if (!writeTask.isNull) {
                        completeTask(*fdi, fdi.write.head, writeTask, Status.Type.Error, ECONNRESET);
                    }

                    epoll_ctl(epoll, EPOLL_CTL_DEL, fd, null);
                    fds.remove(fd);

                    continue;
                }

                if (ev.events & EPOLLIN)
                {
                    fdi.read.ready = true;

                    if (fdi.read.head) {
                        rqueue.put(fdi.read.head);
                    }
                }

                // TODO: Handle partially-completed write operations (and retries in case of EINTR)
                // TODO: Handle EPOLLRDHUP and EPOLLHUP
                if (ev.events & EPOLLOUT)
                {
                    fdi.write.ready = true;

                    auto taskId = fdi.write.head;
                    auto task = tasks.get(taskId);

                    if (!task.isNull)
                    {
                        // TODO: Handle getsockopt() error as well as the fd type
                        if (ev.events & EPOLLERR)
                        {
                            int err;
                            socklen_t len = err.sizeof;
                            getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);

                            task.result = err;
                        }

                        rqueue.put(taskId);
                    }
                }
            }
        }

        /** 
         * Execute all pending tasks from the run queue
         */
        void runTasks() @trusted
        {
            while (!rqueue.empty)
            {
                auto taskId = rqueue.front;
                auto task = tasks.get(taskId);

                if (!task.isNull)
                {
                    auto fdi = task.data.fd in fds;

                    if (fdi) {
                        runTask(*fdi, taskId, task);
                    }
                }

                rqueue.popFront();
            }
        }

    public:
        alias List(Elem...) = Elem;

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
            closefd(epoll);
        }

        static foreach (T; List!(Connect, Accept, Receive, Send, Read, Write))
        {
            IO submit(T op)
            {
                auto entry = tasks.add(toTask(op));
                squeue.put(entry);

                return IO(entry);
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

            processSubmissionQueue();
            runTasks();

            processSubmissionQueue();
            runTasks();

            processEpollEvents();
            runTasks();

            return tasks.length || squeue.length;
        }
    }
