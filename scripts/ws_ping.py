"""실행 중인 로컬 서버에 WebSocket Ping을 보내 Pong을 검증합니다.

사용 예: python3 scripts/ws_ping.py abc
서버 실행/종료는 사용자가 담당합니다. 별도 Python 패키지는 필요 없습니다.
"""

import argparse
import base64
import hashlib
import os
import socket
import sys


def receive_exact(connection, count):
    data = bytearray()
    while len(data) < count:
        chunk = connection.recv(count - len(data))
        if not chunk:
            raise RuntimeError("응답 도중 서버가 연결을 종료했습니다.")
        data.extend(chunk)
    return bytes(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("message", nargs="?", default="abc")
    parser.add_argument("--port", type=int, default=8080)
    args = parser.parse_args()
    payload = args.message.encode("utf-8")
    if len(payload) > 125:
        parser.error("UTF-8로 인코딩한 본문은 125바이트 이하여야 합니다.")

    with socket.create_connection(("127.0.0.1", args.port), timeout=3) as connection:
        key = base64.b64encode(os.urandom(16)).decode("ascii")
        request = (
            "GET / HTTP/1.1\r\n"
            f"Host: 127.0.0.1:{args.port}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            f"Sec-WebSocket-Key: {key}\r\n\r\n"
        )
        connection.sendall(request.encode("ascii"))
        response = bytearray()
        while not response.endswith(b"\r\n\r\n"):
            response.extend(receive_exact(connection, 1))
            if len(response) > 8192:
                raise RuntimeError("핸드셰이크 응답 헤더가 너무 큽니다.")
        lines = bytes(response).decode("ascii").split("\r\n")
        if not lines[0].startswith("HTTP/1.1 101 "):
            raise RuntimeError(f"핸드셰이크 실패: {lines[0]}")
        headers = {}
        for line in lines[1:]:
            if not line:
                break
            name, value = line.split(":", 1)
            headers[name.lower()] = value.strip()
        expected_accept = base64.b64encode(hashlib.sha1(
            (key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode("ascii")
        ).digest()).decode("ascii")
        if headers.get("sec-websocket-accept") != expected_accept:
            raise RuntimeError("Sec-WebSocket-Accept 값이 올바르지 않습니다.")
        print("핸드셰이크 성공: 101 Switching Protocols")

        # 클라이언트 프레임은 MASK=1이며, 새 마스킹 키로 본문을 XOR합니다.
        mask = os.urandom(4)
        masked = bytes(value ^ mask[i % 4] for i, value in enumerate(payload))
        frame = bytes([0x89, 0x80 | len(payload)]) + mask + masked
        connection.sendall(frame)
        print(f"송신 Ping: {args.message!r} ({len(payload)}바이트)")
        print(f"송신 원시 바이트: {frame.hex(' ')}")

        header = receive_exact(connection, 2)
        expected_header = bytes([0x8a, len(payload)])
        if header != expected_header:
            raise RuntimeError(
                f"Pong 헤더 불일치: 기대={expected_header.hex(' ')} 실제={header.hex(' ')}"
            )
        body = receive_exact(connection, len(payload))
        print(f"수신 원시 바이트: {(header + body).hex(' ')}")
        if body != payload:
            raise RuntimeError(f"Pong 본문 불일치: {body!r}")
        print("PASS: FIN=1, opcode=Pong(10), MASK=0, 본문 길이와 내용 일치")
        # 현재 Close 처리는 미구현이므로 테스트가 끝나면 TCP 연결을 닫습니다.


if __name__ == "__main__":
    try:
        main()
    except (OSError, RuntimeError, ValueError) as error:
        print(f"FAIL: {error}", file=sys.stderr)
        sys.exit(1)
