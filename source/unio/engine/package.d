module unio.engine;

@safe @nogc:

public import std.socket : AddressFamily, SocketType, ProtocolType;
public import unio.engine.operations;

public:
    struct IO
    {
        size_t handle;
        alias handle this;
    }

    struct File
    {
        int fd;
        alias fd this;

        /** 
         * On Windows, we transform file descriptors dividing them by 4 (right bitwise shift),
         * because kernel handles are always multiple of 4 and the bottom two bits are useless for us.
         *
         * Source: https://devblogs.microsoft.com/oldnewthing/20050121-00/?p=36633
         */
        size_t toHash() const nothrow
        {
            version (Windows) return fd >> 2;
            else return fd;
        }

        bool opEquals(ref const typeof(this) f) const { return fd == f.fd; }
    }

    struct Socket
    {
        File fd;
        alias fd this;

        this(int fd)
        {
            this.fd = File(fd);
        }
    }

    immutable struct Status
    {
        nothrow @nogc:

        enum Type : byte
        {
            Pending = 0,
            Success = 1,
            Error = -1,
            Running = 2,
        }

        Type type = Type.Pending;
        long result;

        @property
        {
            bool pending() { return type == Type.Pending; }
            bool success() { return type == Type.Success; }
            bool failed() { return type == Type.Error; }
        }
    }

    struct Result
    {
        enum Type : byte { Success = 0, Error = -1 }

        Type type;
        long value;
        alias value this;

        @property
        {
            bool done() const { return type == Type.Success; }
            bool failed() const { return type == Type.Error; }
        }
    }

    struct Event
    {
        IO op;
        Token token;
        Result result;
    }

    /** 
    TODO: Add IO priority support
    TODO: Add operation chaining
    TODO: Add proper fd create and register/unregister functions (socket, fd)
    TODO: Add buffer support (register buffers)
    TODO: Unify operation types and make a single `submit` entry point
    */
    interface IOEngine
    {
    public:
        void open(int fd);
        void close(int fd);

        IO submit(Connect);
        IO submit(Accept);
        IO submit(Receive);
        IO submit(Send);
        IO submit(Read);
        IO submit(Write);
        IO submit(Timeout);

        bool cancel(IO);
        size_t wait(size_t = 0);

        void popFront();

        @property
        {
            size_t length() const;
            bool empty() const;
            Event front();
        }
    }
