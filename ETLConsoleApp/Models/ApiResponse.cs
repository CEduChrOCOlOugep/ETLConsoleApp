namespace ETLConsoleApp.Models
{
    public class ApiResponse
    {
        public List<Item> Row { get; set; }
        public string TotalRecordCount { get; set; }
    }

    public class Item
    {
        public List<Field> Field { get; set; }
    }

    public class Field
    {
        public string Name { get; set; }
        public string Value { get; set; }
    }
}