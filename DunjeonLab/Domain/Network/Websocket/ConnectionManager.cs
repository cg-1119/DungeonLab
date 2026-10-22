using System.Net;
using System.Net.Sockets;

namespace DunjeonLab.Domain.Network.Websocket;

public class ConnectionManager : IDisposable
{
    private readonly Socket _listener;
    
    public ConnectionManager(string address, int port)
    {
        IPAddress ipAddress = IPAddress.Parse(address);
        IPEndPoint endpoint = new IPEndPoint(ipAddress, port);
        
        // TCP 통신에 사용할 소켓 생성
        _listener = new Socket(ipAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
        
        _listener.Bind(endpoint);
        _listener.Listen(128);
    }

    public async Task<Socket> AcceptClientAsync()
    {
        Socket clientSocket = await _listener.AcceptAsync();
        
        return clientSocket;
    }

    public void Dispose()
    {
        _listener.Dispose();
    }
}