module unio.engine.operations;

@safe:// @nogc nothrow:

import unio.engine : File, Socket, IO, IOEngine, Result;

union Token
{
    alias Function = void function (Result);
    alias Delegate = void delegate (Result);

    size_t tag;
    Delegate func;
}

Token toToken(size_t tag)
{
    return Token(tag);
}

Token toToken(Token.Function cb) @trusted
{
    import std.functional : toDelegate;
    Token t = { func: toDelegate(cb) };
    return t;
}

Token toToken(Token.Delegate cb)
{
    Token t = { func: cb };
    return t;
}

mixin template Operation(T)
{
    T fd;
    Token token;
}

public:
    alias OnComplete = void delegate (IOEngine, IO);

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

        this(Socket sock, Address addr)
        {
            fd = sock;
            this.addr = addr;
        }

        this(Socket sock, Address addr, Token token)
        {
            this(sock, addr);
            this.token = token;
        }
    }

    struct Accept
    {
        mixin Operation!Socket;
        Address* newAddr;

        this(Socket sock, Address* newAddr)
        {
            fd = sock;
            this.newAddr = newAddr;
        }

        this(Socket sock, Address* newAddr, Token token)
        {
            this(sock, newAddr);
            this.token = token;
        }
    }

    struct Receive
    {
        mixin Operation!Socket;
        void[] buf;
        int flags;

        this(Socket sock, void[] buf)
        {
            this(sock, buf, 0);
        }

        this(Socket sock, void[] buf, int flags)
        {
            fd = sock;
            this.buf = buf;
            this.flags = flags;
        }

        this(Socket sock, void[] buf, int flags, Token token)
        {
            this(sock, buf, flags);
            this.token = token;
        }

        this(Socket sock, void[] buf, Token token)
        {
            this(sock, buf, 0, token);
        }
    }

    struct Send
    {
        mixin Operation!Socket;
        void[] buf;
        int flags;

        this(Socket sock, void[] buf)
        {
            this(sock, buf, 0);
        }

        this(Socket sock, void[] buf, int flags)
        {
            fd = sock;
            this.buf = buf;
            this.flags = flags;
        }

        this(Socket sock, void[] buf, int flags, Token token)
        {
            this(sock, buf, flags);
            this.token = token;
        }

        this(Socket sock, void[] buf, Token token)
        {
            this(sock, buf, 0, token);
        }
    }

    struct Read
    {
        mixin Operation!File;
        void[] buf;

        this(File file, void[] buf)
        {
            fd = file;
            this.buf = buf;
        }

        this(File file, void[] buf, Token token)
        {
            this(file, buf);
            this.token = token;
        }
    }

    struct Write
    {
        mixin Operation!File;
        void[] buf;

        this(File file, void[] buf)
        {
            fd = file;
            this.buf = buf;
        }

        this(File file, void[] buf, Token token)
        {
            this(file, buf);
            this.token = token;
        }
    }
