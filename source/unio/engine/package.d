module unio.engine;

@safe @nogc:

public import std.socket : AddressFamily, SocketType, ProtocolType;
public import unio.engine.operations;

public:
    immutable struct IO
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

        @property bool pending()
        {
            return type == Type.Pending;
        }

        @property bool success()
        {
            return type == Type.Success;
        }

        @property bool failed()
        {
            return type == Type.Error;
        }
    }

    /** 
    * TODO: add IO priority support
    */
    interface IOEngine
    {
    public:
        IO submit(Connect);
        IO submit(Accept);
        IO submit(Receive);
        IO submit(Send);
        IO submit(Read);
        IO submit(Write);

        bool cancel(IO);
        Status status(IO);

        size_t process();
    }
