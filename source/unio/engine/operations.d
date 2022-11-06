module unio.engine.operations;

@safe:// @nogc nothrow:

import unio.engine : File, Socket, IO, IOEngine;

mixin template Operation(T)
{
    T fd;
    size_t key;
    OnComplete cb;
}

public:
    alias OnComplete = void delegate (IOEngine, immutable IO);

    /** 
     * Socket address struct (sockaddr wrapper)
     */
    struct Address
    {
        import core.sys.posix.arpa.inet;

        sockaddr val;
        alias val this;
    }

    /** 
     * IPv4 address
     */
    struct InetAddr
    {
        import core.sys.posix.netinet.in_;

        Address val;
        alias val this;

        this(string host, ushort port) @trusted
        {
            val = cast(Address) sockaddr_in(
                AF_INET,
                htons(port),
                in_addr(inet_addr(host.ptr))
            );
        }

        this(Address addr)
        {
            val = addr;
        }

        @property uint port()
        {
            return ntohs((cast(sockaddr_in) val).sin_port);
        }

        string toString() const @trusted nothrow
        {
            import core.stdc.string;

            auto str = inet_ntoa((cast(sockaddr_in) val.val).sin_addr);
            auto len = strlen(str);

            return cast(string) str[0 .. len];
        }
    }

    struct Connect
    {
        mixin Operation!Socket;
        Address addr;
    }

    struct Accept
    {
        mixin Operation!Socket;
        Address* newAddr;
    }

    struct Receive
    {
        mixin Operation!Socket;
        void[] buf;
        int flags;
    }

    struct Send
    {
        mixin Operation!Socket;
        void[] buf;
        int flags;
    }

    struct Read
    {
        mixin Operation!File;
        void[] buf;
    }

    struct Write
    {
        mixin Operation!File;
        void[] buf;
    }
