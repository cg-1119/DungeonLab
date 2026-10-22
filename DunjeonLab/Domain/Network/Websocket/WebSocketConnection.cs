using System.Buffers.Binary;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;

namespace DunjeonLab.Domain.Network.Websocket;

public enum PayloadDataType // RFC 6455 기반
{
    Unknown = -1,
    Continuation = 0,
    Text = 1,
    Binary = 2,
    ConnectionClose = 8,
    Ping = 9,
    Pong = 10
}

public enum HandshakeResult
{
    NeedMoreData, // 데이터 부족: 보관하고 다음 수신 기다림
    Completed,    // 프레임 하나 처리 완료
    Invalid       // 잘못된 프레임
}

public enum FrameReadResult
{
    NeedMoreData,
    Completed,
    Invalid
}

public class WebSocketConnection(Socket socket)
{
    public WebSocketState State { get; set; } = WebSocketState.Connecting;

    private readonly byte[] _dataBuffer = new byte[1024];
    private readonly List<byte> _pendingBytes = [];
    private byte[]? _pendingPingPayload;
    private long _pingSequence;
    
    private PayloadDataType? _fragmentedMessageType;
    private readonly List<byte> _messageBytes = [];
    
    private readonly CancellationTokenSource _connectionCts = new();
    private Task? _keepAliveTask;
    public async Task RunAsync()
    {
        while (true)
        {
            int size = await socket.ReceiveAsync(_dataBuffer.AsMemory(), SocketFlags.None); // 데이터 수신 종료

            if (size == 0)
            {
                socket.Close();
                State = WebSocketState.Closed;
                return;
            }
            
            for (int i = 0; i < size; i++)
            {
                _pendingBytes.Add(_dataBuffer[i]);
            }

            if (State == WebSocketState.Connecting)
            {
                HandshakeResult result = await ProcessHandshakeAsync();

                if (result == HandshakeResult.Invalid)
                {
                    socket.Close();
                    State = WebSocketState.Closed;
                    return;
                }
                if (result == HandshakeResult.NeedMoreData)
                {
                    continue; // 다음 ReceiveAsync로 추가 수신
                }
                else if (result == HandshakeResult.Completed)
                {
                    // 주기 루프 시작
                    _keepAliveTask = KeepAliveLoopAsync(_connectionCts.Token);
                }
            }
            
            else if (State == WebSocketState.Open)
            {
                while (true)
                {
                    FrameReadResult result = await ProcessClientRequest();

                    if (result == FrameReadResult.NeedMoreData)
                    {
                        break; // 프레임 처리 반복만 끝내고 추가 수신
                    }

                    if (result == FrameReadResult.Invalid)
                    {
                        socket.Close();
                        State = WebSocketState.Closed;
                        return;
                    }

                    // Completed이면 남아 있는 다음 프레임도 처리
                }
            }
        }
    }
    
    private async Task<HandshakeResult> ProcessHandshakeAsync()
    {
        const int maxHeaderSize = 8192;
        int headerLength = -1;
        
        // 1. HTTP 헤더의 끝인 \r\n\r\n 탐색
        for (int i = 0; i + 3 < _pendingBytes.Count; i++)
        {
            if (_pendingBytes[i] == (byte)'\r' &&
                _pendingBytes[i + 1] == (byte)'\n' &&
                _pendingBytes[i + 2] == (byte)'\r' &&
                _pendingBytes[i + 3] == (byte)'\n')
            {
                headerLength = i + 4;
                break;
            }
        }

        // 2. 제한이 넘지 않는 동안 응답을 더 기다림
        if (headerLength == -1)
        {
            return _pendingBytes.Count >= maxHeaderSize 
                ? HandshakeResult.Invalid 
                : HandshakeResult.NeedMoreData;
        }
        
        if (headerLength > maxHeaderSize) return HandshakeResult.Invalid;
        
        // 3. 완성된 HTTP 헤더 부분만 문자열로 파싱
        byte[] headerBytes = _pendingBytes.GetRange(0, headerLength).ToArray();
        string request = Encoding.ASCII.GetString(headerBytes);
        string[] lines = request.Split("\r\n");
        
        string[] requestLine = lines[0].Split(" ");
        
        if (requestLine.Length != 3 
            || requestLine[0] != "GET" 
            || requestLine[1].Length == 0 
            || requestLine[2] != "HTTP/1.1") return HandshakeResult.Invalid;
        
        // 4. 헤더 이름과 값 분리
        var headers = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var listHeaders = new HashSet<string>(StringComparer.OrdinalIgnoreCase) // 중복 헤더 처리를 위한 집합
        {
            "Connection",
            "Upgrade",
            "Sec-WebSocket-Protocol",
            "Sec-WebSocket-Extensions"
        };

        for (int i = 1; i < lines.Length; i++)
        {
            if (lines[i].Length == 0) break;

            int colonIndex = lines[i].IndexOf(':');

            if (colonIndex <= 0) return HandshakeResult.Invalid;

            string name = lines[i][..colonIndex];
            string value = lines[i][(colonIndex + 1)..].Trim();
            
            if (headers.TryGetValue(name, out string? existingValue))
            {
                // 이미 있는 헤더라도, 목록 형태를 허용하지 않으면 거부
                if (!listHeaders.Contains(name))
                {
                    return HandshakeResult.Invalid;
                }

                // 기존 값의 순서를 유지하면서 새 값을 추가
                headers[name] = existingValue + ", " + value;
            }
            else
            {
                headers.Add(name, value);
            }
        }
        
        // 5. WebSocket 전환에 필요한 헤더를 검사
        if (!headers.TryGetValue("Host", out string? host) ||
            string.IsNullOrWhiteSpace(host) ||
            !headers.TryGetValue("Upgrade", out string? upgrade) ||
            !upgrade.Equals("websocket", StringComparison.OrdinalIgnoreCase) ||
            !headers.TryGetValue("Connection", out string? connection) ||
            !connection.Split(',').Any(
                token => token.Trim().Equals(
                    "Upgrade", StringComparison.OrdinalIgnoreCase)) ||
            !headers.TryGetValue("Sec-WebSocket-Version", out string? version) ||
            version != "13" ||
            !headers.TryGetValue("Sec-WebSocket-Key", out string? key))
        {
            return HandshakeResult.Invalid;
        }

        // 키는 Base64로 표현된 16바이트 값이어야 함
        byte[] decodedKey = new byte[16];

        if (!Convert.TryFromBase64String(key, decodedKey, out int keyLength) ||
            keyLength != 16)
        {
            return HandshakeResult.Invalid;
        }
        
        // 6. 응답 키 계산에는 디코딩한 값이 아닌 원래 키 문자열을 씀
        string acceptSource = key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

        byte[] hash = System.Security.Cryptography.SHA1.HashData(Encoding.ASCII.GetBytes(acceptSource));

        string acceptKey = Convert.ToBase64String(hash);

        byte[] response = Encoding.ASCII.GetBytes(
            "HTTP/1.1 101 Switching Protocols\r\n" +
            "Upgrade: websocket\r\n" +
            "Connection: Upgrade\r\n" +
            $"Sec-WebSocket-Accept: {acceptKey}\r\n" +
            "\r\n");

        // 7. 일부만 전송될 수 있으므로, 남은 부분을 계속 보냄
        int sent = 0;

        while (sent < response.Length)
        {
            int count = await socket.SendAsync(response.AsMemory(sent), SocketFlags.None);

            if (count == 0) return HandshakeResult.Invalid;

            sent += count;
        }

        // 8. 응답 전송이 완료된 뒤 헤더를 소비하고 상태를 바꿉니다.
        _pendingBytes.RemoveRange(0, headerLength);
        State = WebSocketState.Open;

        return HandshakeResult.Completed;
    }

    private async Task<FrameReadResult> ProcessClientRequest()
    {
        if (_pendingBytes.Count < 2)
        {
            return FrameReadResult.NeedMoreData;
        }
        
        byte firstByte = _pendingBytes[0];
        byte secondByte = _pendingBytes[1];
        
        bool fin = (firstByte & 0b10000000) != 0;
        int reservedBits = firstByte & 0b01110000;
        PayloadDataType opcode = (PayloadDataType)(firstByte & 0b00001111);

        bool mask = (secondByte & 0b10000000) != 0;
        int lengthCode = secondByte & 0b01111111;
        
        // 확장을 협상하지 않았으므로 RSV 비트는 모두 0이어야 함
        // 클라이언트가 보내는 프레임은 마스킹되어야 함
        if (reservedBits != 0 || !mask) return FrameReadResult.Invalid;
        if (IsInvalidControlFrame(opcode, lengthCode, fin)) return FrameReadResult.Invalid;
        
        ulong msglen = (ulong)lengthCode;
        ulong offset = 2;
        
        if (lengthCode == 126)
        {
            // 기본 헤더 2 바이트 + 추가 길이 2바이트
            if (_pendingBytes.Count < 4) return FrameReadResult.NeedMoreData;
            
            msglen = ((ulong)_pendingBytes[2] << 8 | _pendingBytes[3]);
            offset = 4;
            
            if (msglen < 126) return FrameReadResult.Invalid;
        }
        else if (msglen == 127)
        {
            // 기본 헤더 2바이트 + 추가 길이 8바이트
            if (_pendingBytes.Count < 10) return FrameReadResult.NeedMoreData;

            if ((_pendingBytes[2] & 0b10000000) != 0) return FrameReadResult.Invalid;

            msglen = 0;

            for (int i = 2; i < 10; i++)
            {
                msglen = (msglen << 8) | _pendingBytes[i];
            }

            offset = 10;

            // 65535 이하라면 더 짧은 길이 형식을 써야 함
            if (msglen < 65536) return FrameReadResult.Invalid;
        }

        const ulong maxPayloadLength = 64 * 1024; // 최대 64KiB까지 허용
        
        if (msglen > maxPayloadLength) return FrameReadResult.Invalid;

        int payloadLength = (int)msglen;
        int maskOffset = (int)offset;
        int payloadOffset = maskOffset + 4;
        int frameLength = payloadOffset + payloadLength;
        
        // 마스킹 키와 본문이 전부 도착하기 전 읽지 않음
        if (_pendingBytes.Count < frameLength) return FrameReadResult.NeedMoreData;
        byte[] decoded = new byte[msglen];
            
        for (int i = 0; i < payloadLength; i++) 
        { 
            decoded[i] = (byte)(_pendingBytes[payloadOffset + i] ^ _pendingBytes[maskOffset + i % 4]);
        }

        switch (opcode)
        {
            case PayloadDataType.Text:
            case PayloadDataType.Binary:
            {
                // 이전 메시지가 끝나지 않았는데 새 메시지가 시작되면 오류
                if (_fragmentedMessageType != null)
                {
                    return FrameReadResult.Invalid;
                }

                if (fin)
                {
                    if (!ProcessCompleteMessage(opcode, decoded)) return FrameReadResult.Invalid;
                }
                else
                {
                    // 다음 Continuation 프레임을 기다림
                    _fragmentedMessageType = opcode;
                    _messageBytes.AddRange(decoded);
                }

                break;
            }
            case PayloadDataType.Continuation:
            {
                if (_fragmentedMessageType == null) return FrameReadResult.Invalid;
                if (_messageBytes.Count + decoded.Length > (int)maxPayloadLength) return FrameReadResult.Invalid;
                
                _messageBytes.AddRange(decoded);

                if (fin)
                {
                    if (!ProcessCompleteMessage(_fragmentedMessageType.Value, _messageBytes.ToArray())) return FrameReadResult.Invalid;
                    
                    _messageBytes.Clear();
                    _fragmentedMessageType = null;
                }
                
                break;
            }
            case PayloadDataType.Ping:
            {
                if (!await SendControlFrameAsync(PayloadDataType.Pong, decoded)) return FrameReadResult.Invalid;
                break;
            }
            case PayloadDataType.Pong:
            {
                break;
            }

            // Continuation과 제어 프레임은 다음 단계에서 작성
        }
        
        
        _pendingBytes.RemoveRange(0, frameLength);
        
        return FrameReadResult.Completed;
    }
    private bool ProcessCompleteMessage(PayloadDataType messageType, byte[] payload)
    {
        if (messageType == PayloadDataType.Text)
        {
            try
            {
                // 잘못된 UTF-8을 대체 문자로 넘기지 않고 오류로 처리
                var utf8 = new UTF8Encoding(false, true);
                string text = utf8.GetString(payload);

                Console.WriteLine($"Text: {text}");
            }
            catch (DecoderFallbackException)
            {
                return false;
            }
        }
        else if (messageType == PayloadDataType.Binary)
        {
            // 바이너리는 문자열로 해석하지 않음
            Console.WriteLine($"Binary: {payload.Length}bytes");
        }
        else
        {
            return false;
        }

        return true;
    }

    private async Task<bool> SendControlFrameAsync(PayloadDataType opcode, byte[] payload)
    {
        if (IsInvalidControlFrame(opcode, payload.Length, true)) return false;

        byte[] frame = new byte[2 + payload.Length];
        
        // FIN과 opcode를 합침
        frame[0] = (byte)(0b10000000 | (byte)opcode);
        
        // 제어 프레임은 본문이 최대 125바이트이므로 길이를 직접 기록
        // 서버 송신 프레임이라 MASK는 0
        frame[1] = (byte)payload.Length;
        payload.CopyTo(frame, 2);

        int sent = 0;

        while (sent < frame.Length)
        {
            int count = await socket.SendAsync(frame.AsMemory(sent), SocketFlags.None);

            if (count == 0) return false;
            
            sent += count;
        }
        
        return true;
    }
    
    private async Task<bool> SendPingAsync()
    {
        // 연결이 열려 있고, 이전 Ping의 응답을 기다리지 않을 때만 전송
        if (State != WebSocketState.Open || _pendingPingPayload is not null)
        {
            return false;
        }

        // 요청 번호 1, 2, 3, ...을 8바이트 빅엔디언 본문
        byte[] payload = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64BigEndian(payload, ++_pingSequence);

        // 빠른 Pong 응답과도 비교할 수 있도록 전송 전에 저장합니다.
        _pendingPingPayload = payload;

        try
        {
            bool sent = await SendControlFrameAsync(PayloadDataType.Ping, payload);
            if (!sent) _pendingPingPayload = null;
            return sent;
        }
        catch
        {
            // 전송 예외가 발생해도 대기 상태를 남기지 않고 호출자에게 알림
            _pendingPingPayload = null;
            throw;
        }
    }

    private async Task KeepAliveLoopAsync(CancellationToken ct)
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));

        try
        {
            while (await timer.WaitForNextTickAsync(ct))
            {
                Console.WriteLine("Ping을 보낼 시점인지 확인");
                // TODO: 전송 시점과 응답 타임아웃 검사
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            // 연결 정료 등 요청한 정상적인 취소
        }
    }

    // 제어 프레임과 유효성 검사 함수
    private static bool IsInvalidControlFrame(PayloadDataType opcode, int len, bool fin)
    {
        return opcode is PayloadDataType.Ping or PayloadDataType.Pong or PayloadDataType.ConnectionClose 
               && (!fin || len > 125);
    }
}
