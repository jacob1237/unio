#!/usr/bin/env dub

/+ dub.sdl:
    name "dumb_http_server"
    dependency "unio" path=".."
    compiler "ldc2"
+/
module examples.dumb_http_server;

import std.stdio;
import unio;

@safe:

enum maxMsgLen = 2048;
enum maxClients = 512;
enum backlog = 512;

void[maxMsgLen] buf;

static response = cast(immutable void[]) (
    "HTTP/1.1 200 OK\r\n" ~
    "Server: unio\r\n" ~
    "Connection: keep-alive\r\n" ~
    "Content-Type: text/plain\r\n" ~
    "Content-Length: 13\r\n" ~
    "\r\n" ~
    "Hello, World!"
);

static error404 = cast(immutable void[]) (
    "HTTP/1.1 404 Not Found\r\n" ~
    "Server: unio\r\n" ~
    "Connection: keep-alive\r\n" ~
    "Content-Type: text/plain\r\n" ~
    "Content-Length: 9\r\n" ~
    "\r\n" ~
    "Not Found"
);

static IOEngine io;

struct Connection
{
private:
    Socket sock;

    void recv()
    {
        io.submit(Receive(sock, buf, toToken(&onRecv)));
    }

    void onRecv(Result result) @trusted
    {
        if (result.failed || result.value == 0) {
            disconnect();
            return;
        }

        const len = result.value;
        const data = cast(ubyte[]) buf;

        static sep = "\r\n\r\n";
        static favicon = "GET /favicon";

        if (len < sep.length) return;

        foreach (const i; 0 .. len - sep.length + 1)
        {
            if (data[0 .. favicon.length] == favicon) {
                send(cast(void[]) error404);
                return;
            }

            if (data[i] == '\r' && data[i .. i + sep.length] == sep) {
                send(cast(void[]) response);
                return;
            }
        }
    }

    void send(void[] resp)
    {
        io.submit(Send(sock, resp, toToken(&onSend)));
    }

    void onSend(Result result)
    {
        recv();
    }

public:
    void connect(Socket sock)
    {
        this.sock = sock;
        recv();
    }

    void disconnect()
    {
        writeln("Connection closed");
        io.close(sock);
    }
}

struct Server
{
private:
    Socket sock;
    InetAddr addr;
    Connection[maxClients] conns;
    bool running;

    void accept()
    {
        io.submit(Accept(sock, &addr.val, toToken(&onAccept)));
    }

    void onAccept(Result result)
    {
        writefln("New connection from %s:%d", addr, addr.port);

        if (result.failed) {
            stop();
            return;
        }

        // Continue accepting connections
        accept();

        const fd = Socket(cast(int) result.value);
        conns[fd].connect(fd);
    }

public:
    this(InetAddr addr) @trusted
    {
        import core.sys.posix.netinet.in_;
        import core.sys.posix.sys.socket;
        import core.sys.linux.netinet.tcp;

        this.addr = addr;

        int flags = 1;
        auto fd = socket(AF_INET, SOCK_STREAM | 0x800, IPPROTO_TCP);

        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flags, int.sizeof);
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &flags, int.sizeof);
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, int.sizeof);

        bind(fd, &addr.val.val, addr.sizeof);
        listen(fd, backlog);
        sock = Socket(fd);
    }

    ~this()
    {
        io.close(sock);
    }

    void stop()
    {
        running = false;
    }

    void run() @trusted
    {
        accept();
        running = true;

        writefln("Server started at http://%s:%d", addr, addr.port);

        while(running)
        {
            io.wait();

            foreach (const event; io) {
                event.token.func(event.result);
            }
        }
    }
}

void main()
{
    io = new EpollEngine();
    Server(InetAddr("0.0.0.0", 8080)).run();
}
