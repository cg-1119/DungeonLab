using System.Net.Sockets;
using DunjeonLab.Domain;
using Raylib_cs;
using DunjeonLab.Domain.Map;
using DunjeonLab.Domain.Network.Websocket;

using var manager = new ConnectionManager("127.0.0.1", 8080);

using Socket client = await manager.AcceptClientAsync();
Console.WriteLine($"클라이언트 접속: {client.RemoteEndPoint}");
var connection = new WebSocketConnection(client);
await connection.RunAsync();


// const int screenWidth = 800;
// const int screenHeight = 600;
// const int tileSize = 50;
//
// Raylib.InitWindow(screenWidth, screenHeight, "DunjeonLab");
// Raylib.SetTargetFPS(60);
//
// Map map = new Map();
//
// while (!Raylib.WindowShouldClose())
// {
//     Raylib.BeginDrawing();
//     Raylib.ClearBackground(Color.Black);
//     for (int row = 0; row < map.arr.GetLength(0); row++)
//     {
//         for (int column = 0; column < map.arr.GetLength(1); column++)
//         {
//             Color color = map.arr[row, column] == Tile.Wall
//                 ? Color.DarkGray
//                 : Color.Beige;
//
//             int x = column * tileSize;
//             int y = row * tileSize;
//
//             Raylib.DrawRectangle(x, y, tileSize, tileSize, color);
//             Raylib.DrawRectangleLines(x, y, tileSize, tileSize, Color.Black);
//         }
//     }
//
//     Raylib.EndDrawing();
// }
//
// Raylib.CloseWindow();
