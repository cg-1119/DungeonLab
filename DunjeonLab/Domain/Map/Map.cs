namespace DunjeonLab.Domain.Map;

public class Map
{
    public Tile[,] arr =
    {
        { Tile.Wall, Tile.Wall,  Tile.Wall,  Tile.Wall,  Tile.Wall },
        { Tile.Wall, Tile.Floor, Tile.Floor, Tile.Floor, Tile.Wall },
        { Tile.Wall, Tile.Floor, Tile.Wall,  Tile.Floor, Tile.Wall },
        { Tile.Wall, Tile.Floor, Tile.Floor, Tile.Floor, Tile.Wall },
        { Tile.Wall, Tile.Wall,  Tile.Wall,  Tile.Wall,  Tile.Wall }
    };

}