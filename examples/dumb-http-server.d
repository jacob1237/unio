#!/bin/env dub
/+ dub.sdl:
    name: "dumb-http-server"
+/

import std.stdio;
import unio;

@safe:

enum maxMsgLen = 2048;
enum maxClients = 512;
enum backlog = 512;

void[maxMsgLen] buf;

static response = cast(immutable void[]) (
    "HTTP/1.1 200 OK\r\n"
    ~ "Server: unio\r\n"
    ~ "Connection: keep-alive\r\n"
    ~ "Content-Type: text/plain\r\n"
    ~ "Content-Length: 13\r\n"
    ~ "\r\n"
    ~ "Hello, World!"
);

static error404 = cast(immutable void[]) (
    "HTTP/1.1 404 Not Found\r\n"
    ~ "Server: unio\r\n"
    ~ "Connection: keep-alive\r\n"
    ~ "Content-Type: text/plain\r\n"
    ~ "Content-Length: 9\r\n"
    ~ "\r\n"
    ~ "Not Found"
);

struct Connection
{
private:
    Socket sock;
    int len;
    bool eof;

    void handleRequest(IOEngine io, size_t len) @trusted
    {
        static sep = "\r\n\r\n";
        static favicon = "GET /favicon";

        if (len < 4) {
            return;
        }

        auto data = cast(ubyte[]) buf;

        for (auto i = 0; i <= len - 4; ++i)
        {
            if (data[0 .. favicon.length] == favicon) {
                send(io, error404);
                return;
            }

            if (data[i] == '\r' && data[i .. i + 4] == sep) {
                send(io, response);
                return;
            }
        }
    }

    void onRecv(IOEngine io, IO op) @trusted
    {
        with (io.status(op))
        {
            if (failed || result == 0) {
                close();
                return;
            }

            handleRequest(io, result);
        }
    }

    void onSend(IOEngine io, IO op)
    {
        io.status(op); // Cleanup
        recv(io);
    }

    void recv(IOEngine io)
    {
        io.submit(Receive(sock, 0, &onRecv, buf));
    }

    void send(IOEngine io, immutable void[] resp) @trusted
    {
        io.submit(Send(sock, 0, &onSend, cast(void[]) resp));
    }

public:
    void close()
    {
        // writeln("Connection closed");
        closeSocket(sock);
    }
}

struct Server
{
private:
    IOEngine io;
    Socket sock;
    Address addr;
    Connection[maxClients] conns;
    bool running;

    void onConnect(IOEngine io, IO op)
    {
        // auto inAddr = InetAddr(addr);
        // writefln("New connection: %s:%d", inAddr, inAddr.port);

        auto status = io.status(op);

        if (status.failed) {
            stop();
            return;
        }

        immutable fd = Socket(cast(int) status.result);
        auto conn = &conns[fd];
        conn.sock = fd;
        conn.recv(io);

        accept();
    }

    void accept()
    {
        io.submit(Accept(sock, 0, &onConnect, &addr));
    }

public:
    this(IOEngine engine, Address addr) @trusted
    {
        import core.sys.posix.netinet.in_;
        import core.sys.posix.sys.socket;
        import core.sys.linux.netinet.tcp;

        io = engine;

        int flags = 1;
        auto fd = socket(AF_INET, SOCK_STREAM | 0x800, IPPROTO_TCP);

        setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &flags, int.sizeof);
        setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &flags, int.sizeof);
        setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &flags, int.sizeof);

        bind(fd, &addr.val, addr.sizeof);
        listen(fd, backlog);
        sock = Socket(fd);
    }

    ~this()
    {
        closeSocket(sock);
    }

    void stop()
    {
        running = false;
    }

    void run()
    {
        accept();

        running = true;
        while(running && io.process()) {}
    }
}

void closeSocket(Socket s)
{
    import core.sys.posix.unistd : close;
    close(s);
}

void main()
{
    Server(
        new EpollEngine(),
        InetAddr("0.0.0.0", 8080)
    ).run();
}
