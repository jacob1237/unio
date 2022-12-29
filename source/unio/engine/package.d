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
    * TODO: add IO priority support
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

        bool cancel(IO);
        size_t wait(size_t = 0);

        void popFront();

        @property
        {
            size_t length() const;
            bool empty() const;
            Event front() const;
        }
    }
